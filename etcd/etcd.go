package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/clientv3"
	"registry"
)

/*
注册流程：
1. run 如果当前管道channel中有服务需要注册(不在map中)，就放到map保存
2. 遍历map中的服务：看resgester
		如果服务已注册，检查已注册的service的keepalive管道
		如果服务未注册或者，对service做注册操作
3. 对已注册的服务做etcd的LeaseKeepAliveResponse：
		如果etcd返回的keepalive管道有数据表示服务存活，不做处理
		如果etcd返回的keepalive管道没有数据表示服务挂了，将resgester状态改为未注册
4. service做注册操作：
		获取租约授权
		遍历service的所有节点，对每个节点做注册并keepalive操作
5. 在run方法里面定时同步etcd的service信息到atomic.Value原子缓存 方便服务发现
*/

/*
服务发现(通过服务名获取服务)：
1. 先从缓存中获取服务，缓存中没有则从etcd中获取
2. 考虑并发加锁处理：如果100个并发发现缓存中都没有服务，就都会从etcd中取，这样会浪费带宽和资源
3. 加锁方案：缓存没取到时首先加锁，再检测是否已经从etcd中加载成功了，如果没有则从etcd获取
4. 从etcd获取服务：
		获取指定名字的服务path
		根据path从etcd获取服务信息
*/

const (
	MaxServiceNum          = 8
	MaxSyncServiceInterval = time.Second * 30 // 异步更新的时间间隔
)

//etcd 注册插件
type EtcdRegistry struct {
	options   *registry.Options
	client    *clientv3.Client
	serviceCh chan *registry.Service // channel 存储service

	value              atomic.Value
	lock               sync.Mutex
	registryServiceMap map[string]*RegisterService // map保存要注册的所有service
}

// 保存所有服务的结构体 服务名称对应的服务信息
type AllServiceInfo struct {
	serviceMap map[string]*registry.Service
}

// 要注册的服务结构体
type RegisterService struct {
	id          clientv3.LeaseID
	service     *registry.Service
	registered  bool // 标记是否已注册
	keepAliveCh <-chan *clientv3.LeaseKeepAliveResponse
}

// 初始化插件的结构体
var (
	etcdRegistry *EtcdRegistry = &EtcdRegistry{
		serviceCh:          make(chan *registry.Service, MaxServiceNum),
		registryServiceMap: make(map[string]*RegisterService, MaxServiceNum),
	}
)

func init() {
	allServiceInfo := &AllServiceInfo{
		serviceMap: make(map[string]*registry.Service, MaxServiceNum),
	}

	etcdRegistry.value.Store(allServiceInfo)
	err := registry.RegisterPlugin(etcdRegistry)
	if err != nil {
		return
	}
	go etcdRegistry.run()
}

//插件的名字
func (e *EtcdRegistry) Name() string {
	return "etcd"
}

//初始化
func (e *EtcdRegistry) Init(ctx context.Context, opts ...registry.Option) (err error) {

	e.options = &registry.Options{}
	for _, opt := range opts {
		opt(e.options)
	}

	e.client, err = clientv3.New(clientv3.Config{
		Endpoints:   e.options.Addrs,
		DialTimeout: e.options.Timeout,
	})

	if err != nil {
		err = fmt.Errorf("init etcd failed, err:%v", err)
		return
	}

	return
}

//把要注册的service保存到channel中
func (e *EtcdRegistry) Register(ctx context.Context, service *registry.Service) (err error) {
	select {
	case e.serviceCh <- service:
	default:
		err = fmt.Errorf("register chan is full")
		return
	}
	return
}

//服务反注册
func (e *EtcdRegistry) Unregister(ctx context.Context, service *registry.Service) (err error) {
	return
}

// 后台线程不断从channel管道取 如果没有注册则保存到map
func (e *EtcdRegistry) run() {
	// time.NewTicker 是golang的定时器  每隔间隔时间会触发
	ticker := time.NewTicker(MaxSyncServiceInterval)
	for {
		// 从channel管道读出服务
		select {
		case service := <-e.serviceCh:
			// 判断服务是否在map存在
			registryService, ok := e.registryServiceMap[service.Name]
			if ok {
				// 更新服务对应的节点信息
				for _, node := range service.Nodes {
					registryService.service.Nodes = append(registryService.service.Nodes, node)
				}
				registryService.registered = false
				break
			}
			// 如果不存在则放到map中
			registryService = &RegisterService{
				service: service,
			}
			e.registryServiceMap[service.Name] = registryService
		case <-ticker.C: // 到了定时器时间间隔 触发定时器
			// 同步etcd的服务信息到缓存
			e.syncServiceFromEtcd()
		default:
			// 如果管道channel没有值  做续约/注册 操作
			e.registerOrKeepAlive()
			time.Sleep(time.Millisecond * 500)
		}
	}
}

// 判断service是续约还是注册
func (e *EtcdRegistry) registerOrKeepAlive() {
	// 遍历registryServiceMap的所有数据 如果服务已经注册则保持续约 否则注册
	for _, registryService := range e.registryServiceMap {
		if registryService.registered {
			e.keepAlive(registryService)
			continue
		}

		e.registerService(registryService)
	}
}

func (e *EtcdRegistry) keepAlive(registryService *RegisterService) {
	// 已注册的service做续约操作
	select {
	case resp := <-registryService.keepAliveCh:
		if resp == nil {
			registryService.registered = false
			return
		}
	}
	return
}

// 将服务注册到Etcd  给EtcdRegistry这个插件定义的方法
func (e *EtcdRegistry) registerService(registryService *RegisterService) (err error) {
	// 获取租约授权
	resp, err := e.client.Grant(context.TODO(), e.options.HeartBeat)
	if err != nil {
		return
	}

	registryService.id = resp.ID
	// 服务注册到多个节点
	for _, node := range registryService.service.Nodes {
		// 每个节点信息保存到结构体
		tmp := &registry.Service{
			Name: registryService.service.Name,
			Nodes: []*registry.Node{
				node,
			},
		}

		data, err := json.Marshal(tmp)
		if err != nil {
			continue
		}
		// 拿到节点的key 节点服务地址信息
		key := e.serviceNodePath(tmp)
		fmt.Printf("register key:%s\n", key)
		// 用租约设置key 注册
		_, err = e.client.Put(context.TODO(), key, string(data), clientv3.WithLease(resp.ID))
		if err != nil {
			continue
		}

		// 自动续期  KeepAlive 永久续约
		ch, err := e.client.KeepAlive(context.TODO(), resp.ID)
		if err != nil {
			continue
		}

		registryService.keepAliveCh = ch
		registryService.registered = true
	}

	return
}

// 注册的服务的path信息
func (e *EtcdRegistry) serviceNodePath(service *registry.Service) string {
	// 通过注册服务拿到节点的path信息
	nodeIP := fmt.Sprintf("%s:%d", service.Nodes[0].IP, service.Nodes[0].Port)
	return path.Join(e.options.RegistryPath, service.Name, nodeIP)
}

// 获取指定名字的服务的path
func (e *EtcdRegistry) servicePath(name string) string {
	return path.Join(e.options.RegistryPath, name)
}

// 从缓存中获取指定名字的服务信息
func (e *EtcdRegistry) getServiceFromCache(ctx context.Context,
	name string) (service *registry.Service, ok bool) {

	allServiceInfo := e.value.Load().(*AllServiceInfo)
	//一般情况下，都会从缓存中读取
	service, ok = allServiceInfo.serviceMap[name]
	return
}

func (e *EtcdRegistry) GetService(ctx context.Context,
	name string) (service *registry.Service, err error) {

	//一般情况下，都会从缓存中读取
	service, ok := e.getServiceFromCache(ctx, name)
	if ok {
		return
	}

	//如果缓存中没有这个service，则从etcd中读取
	e.lock.Lock()
	defer e.lock.Unlock()
	//先检测，是否已经从etcd中加载成功了
	service, ok = e.getServiceFromCache(ctx, name)
	if ok {
		return
	}

	//从etcd中读取指定服务名字的服务信息
	key := e.servicePath(name)
	// 从etcd获取服务
	resp, err := e.client.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return
	}

	service = &registry.Service{
		Name: name,
	}

	for _, kv := range resp.Kvs {
		value := kv.Value
		var tmpService registry.Service
		err = json.Unmarshal(value, &tmpService)
		if err != nil {
			return
		}

		for _, node := range tmpService.Nodes {
			service.Nodes = append(service.Nodes, node)
		}
	}

	allServiceInfoOld := e.value.Load().(*AllServiceInfo)
	//var allServiceInfoNew = &AllServiceInfo{
	//	serviceMap: make(map[string]*registry.Service, MaxServiceNum),
	//}
	//
	//for key, val := range allServiceInfoOld.serviceMap {
	//	allServiceInfoNew.serviceMap[key] = val
	//}
	//
	//allServiceInfoNew.serviceMap[name] = service
	allServiceInfoOld.serviceMap[name] = service
	e.value.Store(allServiceInfoOld)
	//e.value.Store(allServiceInfoNew)
	return
}

// 定时从etcd同步service信息到原子的atomic.Value缓存
func (e *EtcdRegistry) syncServiceFromEtcd() {
	var allServiceInfoNew = &AllServiceInfo{
		serviceMap: make(map[string]*registry.Service, MaxServiceNum),
	}
	// 从原子的atomic.Value取出所有缓存信息
	ctx := context.TODO()
	allServiceInfo := e.value.Load().(*AllServiceInfo)

	//对于缓存的每一个服务，都需要从etcd中进行更新
	for _, service := range allServiceInfo.serviceMap {
		key := e.servicePath(service.Name)
		resp, err := e.client.Get(ctx, key, clientv3.WithPrefix())
		if err != nil {
			allServiceInfoNew.serviceMap[service.Name] = service
			continue
		}

		serviceNew := &registry.Service{
			Name: service.Name,
		}

		for _, kv := range resp.Kvs {
			value := kv.Value
			var tmpService registry.Service
			err = json.Unmarshal(value, &tmpService)
			if err != nil {
				fmt.Printf("unmarshal failed, err:%v value:%s", err, string(value))
				return
			}

			for _, node := range tmpService.Nodes {
				serviceNew.Nodes = append(serviceNew.Nodes, node)
			}
		}
		allServiceInfoNew.serviceMap[serviceNew.Name] = serviceNew
	}

	// 从etcd更新服务信息之后保存到原子value缓存
	e.value.Store(allServiceInfoNew)
}
