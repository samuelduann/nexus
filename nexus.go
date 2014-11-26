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
	"sync"
	"time"
)

type GoshineAgent struct {
	id   int
	gs   *goshine.Goshine
	lock sync.Mutex
}

type Nexus struct {
	agentPool []*GoshineAgent
	l1cache   *L1Cache
	config    *Config
}

type NxRequest struct {
	Hql string
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

func (nx *Nexus) makeResponse(w http.ResponseWriter, r *http.Request, response *NxResponse) {
	data, _ := json.Marshal(response)
	w.Write(data)
	if response.Code == 200 {
		logger.Infof("%s %s - %s %d %s", r.RemoteAddr, r.Method, r.URL, response.Code, response.Message)
	} else {
		logger.Warnf("%s %s - %s %d %s", r.RemoteAddr, r.Method, r.URL, response.Code, response.Message)
	}
}

func (nx *Nexus) parseRequest(r *http.Request) *NxRequest {
	r.ParseForm()
	hql, ok := r.Form["hql"]
	if !ok || len(hql[0]) == 0 {
		return nil
	}
	return &NxRequest{Hql: hql[0]}
}

func (nx *Nexus) QueryHandler(w http.ResponseWriter, r *http.Request) {
	request := nx.parseRequest(r)
	if request == nil {
		nx.makeResponse(w, r, &Response_400)
		return
	}
	// try fetch from cache
	if result := nx.queryCache(request); result != nil {
		nx.makeResponse(w, r, &NxResponse{Code: 200, Message: "Ok", Body: result})
		return
	}
	// get an agent
	g := nx.assignAgent(request)
	if g == nil {
		nx.makeResponse(w, r, &Response_503)
		return
	}
	defer func() {
		if err := recover(); err != nil {
			nx.freeAgent(g)
			nx.makeResponse(w, r, &Response_500)
		}
	}()
	// try fetch from cache again, in a very special case
	if result := nx.queryCache(request); result != nil {
		nx.freeAgent(g)
		nx.makeResponse(w, r, &NxResponse{Code: 200, Message: "Ok", Body: result})
		return
	}
	//
	result, err := g.gs.FetchAll(request.Hql)
	// put agent back
	nx.freeAgent(g)
	if err != nil {
		nx.makeResponse(w, r, &NxResponse{Code: 500, Message: fmt.Sprintf("%s", err)})
		return
	}
	// put cache
	nx.updateCache(request, result)
	// return result
	nx.makeResponse(w, r, &NxResponse{Code: 200, Message: "Ok", Body: result})
}

func (nx *Nexus) ExecuteHandler(w http.ResponseWriter, r *http.Request) {
	request := nx.parseRequest(r)
	if request == nil {
		nx.makeResponse(w, r, &Response_400)
		return
	}
	// get an agent and execute
	g := nx.assignAgent(request)
	if g == nil {
		nx.makeResponse(w, r, &Response_503)
		return
	}
	if err := g.gs.Execute(request.Hql); err != nil {
		nx.makeResponse(w, r, &NxResponse{Code: 500, Message: fmt.Sprintf("%s", err)})
	} else {
		nx.makeResponse(w, r, &Response_200)
	}
	nx.freeAgent(g)
}

func (nx *Nexus) initAgentPool() error {
	nx.agentPool = make([]*GoshineAgent, len(nx.config.GoshineList), len(nx.config.GoshineList))
	for i, gconf := range nx.config.GoshineList {
		g := goshine.NewGoshine(gconf.Host, gconf.Port, "user0", "passwd0", nx.config.Database)
		if err := g.Connect(); err != nil {
			logger.Warnf("init goshine agent%d %s:%d failed, err: %s", i, gconf.Host, gconf.Port, err)
			return errors.New("init goshine failed")
		}
		logger.Infof("goshine agent%d %s:%d activated", i, gconf.Host, gconf.Port)
		nx.agentPool[i] = &GoshineAgent{id: i, gs: g}
	}
	go nx.agentMaintenance()
	return nil
}

func (nx *Nexus) getAgentId(req *NxRequest) int {
	checksum := 0
	for _, chr := range req.Hql {
		checksum = (checksum + int(chr)) % len(nx.agentPool)
	}
	return checksum
}

func (nx *Nexus) assignAgent(request *NxRequest) *GoshineAgent {
	//map request to an available agent id
	i := nx.getAgentId(request)
	id := -1
	for skip := 0; skip < len(nx.agentPool); skip++ {
		if nx.agentPool[(i+skip)%len(nx.agentPool)].gs.GetStatus() == goshine.GS_STATUS_CONNECTED {
			id = (i + skip) % len(nx.agentPool)
			break
		}
		logger.Warnf("agent%d is down, try next..", i)
	}
	if id == -1 {
		return nil
	}
	// try get control of that agent
	nx.agentPool[id].lock.Lock()
	return nx.agentPool[id]
}

func (nx *Nexus) freeAgent(ga *GoshineAgent) {
	ga.lock.Unlock()
}

func (nx *Nexus) agentMaintenance() {
	// check out agents which is at error state, and try reconnect
	ticker := time.Tick(10 * time.Second)
	for {
		<-ticker
		for i, ga := range nx.agentPool {
			if ga.gs.GetStatus() == goshine.GS_STATUS_ERROR {
				logger.Warnf("agent%d is in error status, trying to recover...", i)
				if err := ga.gs.Connect(); err != nil {
					logger.Warnf("agent%d recovering failed err: %s", i, err)
				} else {
					logger.Warnf("agent%d recovered", i)
				}
			}
		}
	}
}

func (nx *Nexus) Start() bool {
	if nx.initAgentPool() != nil {
		logger.Warnf("initAgentPool failed, abort")
		return false
	}
	http.HandleFunc("/query", nx.QueryHandler)
	http.HandleFunc("/execute", nx.ExecuteHandler)
	addr := fmt.Sprintf("%s:%d", nx.config.Host, nx.config.Port)
	logger.Infof("staring nexus at %s", addr)
	http.ListenAndServe(addr, nil)
	return true
}
