package nexus

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/samuelduann/goshine"
	"github.com/samuelduann/log4g"
	"io"
	"net/http"
	"runtime/debug"
	"sync"
	"time"
)

type GoshineAgent struct {
	id   int
	gs   *goshine.Goshine
	lock sync.Mutex
}

type Nexus struct {
	clusters map[string][]*GoshineAgent
	l1cache  *L1Cache
	config   *Config
}

type NxRequest struct {
	Hql         string
	ClusterName string
}

type NxResponse struct {
	Code    int
	Message string
	Body    interface{}
}

var (
	Response_503 = NxResponse{Code: 503, Message: "System Is Down", Body: ""}
	Response_500 = NxResponse{Code: 500, Message: "Internal Server Error", Body: ""}
	Response_400 = NxResponse{Code: 400, Message: "Bad Request", Body: ""}
	Response_200 = NxResponse{Code: 200, Message: "Ok", Body: ""}
	Response_123 = NxResponse{Code: 123, Message: "It doesn't works!", Body: ""}
)

var logger *log4g.Logger

func NewNexus(confFilename string) *Nexus {
	n := &Nexus{}
	n.config = GetConfig()
	n.config.Load(confFilename)
	n.l1cache = NewL1Cache()
	logger = log4g.NewLogger(n.config.LogPrefix, log4g.FilenameSuffixInHour)
	return n
}

func (nx *Nexus) getCacheKey(req *NxRequest) string {
	m := md5.New()
	io.WriteString(m, req.Hql)
	return hex.EncodeToString(m.Sum(nil))
}

func (nx *Nexus) updateCache(req *NxRequest, res *goshine.GsResultSet) {
	nx.l1cache.Set(nx.getCacheKey(req), res, nx.config.CacheTimeout)
}

func (nx *Nexus) queryCache(req *NxRequest) *goshine.GsResultSet {
	ret, _ := nx.l1cache.Get(nx.getCacheKey(req))
	if ret != nil {
		return ret.(*goshine.GsResultSet)
	}
	return nil
}

func (nx *Nexus) makeResponse(w http.ResponseWriter, r *http.Request, response *NxResponse, timing *Timing) {
	data, _ := json.Marshal(response)
	w.Write(data)
	if response.Code == 200 {
		logger.Infof("%s %s %s %d %s [%s]", r.RemoteAddr, r.Method, r.URL, response.Code, response.Message, timing)
	} else {
		logger.Warnf("%s %s %s %d %s [%s]", r.RemoteAddr, r.Method, r.URL, response.Code, response.Message, timing)
	}
}

func (nx *Nexus) parseRequest(r *http.Request) *NxRequest {
	r.ParseForm()
	hql, ok := r.Form["hql"]
	if !ok || len(hql[0]) == 0 {
		return nil
	}
	cluster, ok := r.Form["cluster"]
	if !ok || len(cluster[0]) == 0 {
		return nil
	}
	return &NxRequest{Hql: hql[0], ClusterName: cluster[0]}
}

func (nx *Nexus) QueryHandler(w http.ResponseWriter, r *http.Request) {
	timing := NewTiming()
	request := nx.parseRequest(r)
	if request == nil {
		nx.makeResponse(w, r, &Response_400, timing)
		return
	}
	// try fetch from cache
	timing.TickStart("ReadCache1")
	if result := nx.queryCache(request); result != nil {
		nx.makeResponse(w, r, &NxResponse{Code: 200, Message: "Ok", Body: result}, timing)
		return
	}
	timing.TickStop("ReadCache1")

	// get an agent
	timing.TickStart("AssignAgent")
	g, err := nx.assignAgent(request)
	if err != nil {
		nx.makeResponse(w, r, &NxResponse{Code: 504, Message: fmt.Sprintf("%s", err)}, timing)
		return
	}
	defer func() {
		if err := recover(); err != nil {
			nx.freeAgent(g)
			nx.makeResponse(w, r, &Response_500, timing)
			if nx.config.DebugMode == true {
				debug.PrintStack()
			}
		}
	}()
	timing.TickStop("AssignAgent")

	timing.TickStart("ReadCache2")
	// try fetch from cache again, in a very special case
	if result := nx.queryCache(request); result != nil {
		nx.freeAgent(g)
		nx.makeResponse(w, r, &NxResponse{Code: 200, Message: "Ok", Body: result}, timing)
		return
	}
	timing.TickStop("ReadCache2")

	timing.TickStart("Spark")
	result, err := g.gs.FetchAll(request.Hql)
	timing.TickStop("Spark")

	// put agent back
	nx.freeAgent(g)

	if err != nil {
		nx.makeResponse(w, r, &NxResponse{Code: 500, Message: fmt.Sprintf("%s", err)}, timing)
		return
	}

	// put cache
	timing.TickStart("UpdateCache")
	nx.updateCache(request, result)
	timing.TickStop("UpdateCache")

	// return result
	nx.makeResponse(w, r, &NxResponse{Code: 200, Message: "Ok", Body: result}, timing)
}

func (nx *Nexus) ExecuteHandler(w http.ResponseWriter, r *http.Request) {
	timing := NewTiming()
	request := nx.parseRequest(r)
	if request == nil {
		nx.makeResponse(w, r, &Response_400, timing)
		return
	}
	// get an agent and execute
	timing.TickStart("AssignAgent")
	g, err := nx.assignAgent(request)
	if err != nil {
		nx.makeResponse(w, r, &NxResponse{Code: 504, Message: fmt.Sprintf("%s", err)}, timing)
		return
	}
	timing.TickStop("AssignAgent")

	timing.TickStart("Spark")
	if err := g.gs.Execute(request.Hql); err != nil {
		nx.makeResponse(w, r, &NxResponse{Code: 500, Message: fmt.Sprintf("%s", err)}, timing)
	} else {
		nx.makeResponse(w, r, &Response_200, timing)
	}
	nx.freeAgent(g)
}

func (nx *Nexus) initClusters() error {
	nx.clusters = make(map[string][]*GoshineAgent)
	for cluster_name, cluster_config := range nx.config.GoshineClusters {
		nx.clusters[cluster_name] = make([]*GoshineAgent, len(cluster_config), len(cluster_config))
		for i, gconf := range cluster_config {
			g := goshine.NewGoshine(gconf.Host, gconf.Port, "user0", "passwd0", nx.config.Database)
			if err := g.Connect(); err != nil {
				logger.Warnf("init goshine agent %s:%d %s:%d failed, err: %s", cluster_name, i, gconf.Host, gconf.Port, err)
				return errors.New("init goshine failed")
			}
			logger.Infof("goshine agent %s:%d %s:%d activated", cluster_name, i, gconf.Host, gconf.Port)
			nx.clusters[cluster_name][i] = &GoshineAgent{id: i, gs: g}
		}
	}
	go nx.agentMaintenance()
	return nil
}

func (nx *Nexus) getAgentId(req *NxRequest, cluster []*GoshineAgent) int {
	checksum := 0
	for _, chr := range req.Hql {
		checksum = (checksum + int(chr)) % len(cluster)
	}
	return checksum
}

func (nx *Nexus) assignAgent(request *NxRequest) (*GoshineAgent, error) {
	// check if the cluster exists
	cluster, ok := nx.clusters[request.ClusterName]
	if !ok {
		logger.Warnf("cluster %s does not exists", request.ClusterName)
		return nil, errors.New("No Such Cluster")
	}
	//map request to an available agent id
	i := nx.getAgentId(request, cluster)
	id := -1
	for skip := 0; skip < len(cluster); skip++ {
		id = (i + skip) % len(cluster)
		if cluster[id].gs.GetStatus() == goshine.GS_STATUS_CONNECTED {
			break
		}
		logger.Warnf("agent %s:%d is down, trying next..", request.ClusterName, i)
	}
	if id == -1 {
		return nil, errors.New("System Is Down")
	}
	// try get control of that agent
	cluster[id].lock.Lock()
	return cluster[id], nil
}

func (nx *Nexus) freeAgent(ga *GoshineAgent) {
	ga.lock.Unlock()
}

func (nx *Nexus) agentMaintenance() {
	// check out agents which is at error state, and try reconnect
	ticker := time.Tick(10 * time.Second)
	for {
		<-ticker
		for cluster_name, gas := range nx.clusters {
			for i, ga := range gas {
				if ga.gs.GetStatus() == goshine.GS_STATUS_ERROR {
					logger.Warnf("agent %s:%d is in error status, trying to recover...", cluster_name, i)
					if err := ga.gs.Connect(); err != nil {
						logger.Warnf("agent%d recovering failed err: %s", i, err)
					} else {
						logger.Warnf("agent%d recovered", i)
					}
				}
			}
		}
	}
}

func (nx *Nexus) Start() bool {
	if nx.initClusters() != nil {
		logger.Warnf("initClusters failed, abort")
		return false
	}
	http.HandleFunc("/query", nx.QueryHandler)
	http.HandleFunc("/execute", nx.ExecuteHandler)
	addr := fmt.Sprintf("%s:%d", nx.config.Host, nx.config.Port)
	logger.Infof("staring nexus at %s", addr)
	http.ListenAndServe(addr, nil)
	return true
}
