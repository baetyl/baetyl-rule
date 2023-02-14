package rule

import (
	"fmt"
	"time"

	"github.com/256dpi/gomqtt/packet"
	"github.com/baetyl/baetyl-go/v2/errors"
	"github.com/baetyl/baetyl-go/v2/http"
	"github.com/baetyl/baetyl-go/v2/log"
	"github.com/baetyl/baetyl-go/v2/mqtt"
	"github.com/baetyl/baetyl-go/v2/utils"
	routing "github.com/qiangxue/fasthttp-routing"
	"github.com/valyala/fasthttp"

	"github.com/baetyl/baetyl-rule/v2/client"
)

type ServerConfig struct {
	Port int32 `yaml:"port" json:"port"`
	utils.Certificate
}

type HTTPServer struct {
	name        string
	cfg         *ServerConfig
	server      *http.Server
	functionCli *http.Client
	rulers      map[string]RuleInfo      // key: rule name
	client      map[string]client.Client // key: client name
	logger      *log.Logger
}

func NewHTTPServer(info ClientInfo, functionCli *http.Client) (*HTTPServer, error) {
	cfg := new(ServerConfig)
	err := info.Parse(cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	svc := &HTTPServer{
		name:        info.Name,
		cfg:         cfg,
		functionCli: functionCli,
		rulers:      map[string]RuleInfo{},
		client:      map[string]client.Client{},
		logger:      log.With(log.Any("http server", info.Name)),
	}
	svc.server = http.NewServer(http.ServerConfig{
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}, svc.initRouter())
	return svc, nil
}

func (h *HTTPServer) initRouter() fasthttp.RequestHandler {
	router := routing.New()
	router.Post("/rules/<ruleName>", Wrapper(h.HandleHTTPRule))
	return router.HandleRequest
}

func (h *HTTPServer) HandleHTTPRule(ctx *routing.Context) (interface{}, error) {
	var err error
	ruleName := ctx.Param("ruleName")
	ruleInfo, ok := h.rulers[ruleName]
	if !ok {
		err = errors.New("rule name not found")
		http.RespondMsg(ctx, 400, "RequestParamInvalid", err.Error())
		return nil, errors.Trace(err)
	}
	data := ctx.Request.Body()
	if ruleInfo.Function != nil {
		data, err = h.functionCli.Call(ruleInfo.Function.Name, ctx.Request.Body())
		if err != nil {
			http.RespondMsg(ctx, 500, "Failed to call function", err.Error())
			return nil, errors.Trace(err)
		}
	}
	if ruleInfo.Target != nil && len(data) != 0 {
		out := mqtt.NewPublish()
		out.Message = packet.Message{
			Topic:   RegularPubTopic("", "", ruleInfo.Target.Topic, ruleInfo.Target.Path),
			Payload: data,
			QOS:     0,
		}
		err = h.client[ruleInfo.Target.Client].SendOrDrop(ruleInfo.Target.Method, out)
		if err != nil {
			http.RespondMsg(ctx, 500, "Failed to send to target", err.Error())
			return nil, errors.Trace(err)
		}
		h.logger.Debug("send pkt to target in source", log.Any("pkt", out))
	}
	return map[string]bool{
		"success": true,
	}, nil
}

func (h *HTTPServer) Start() {
	if len(h.rulers) == 0 {
		return
	}
	go func() {
		address := fmt.Sprintf(":%d", h.cfg.Port)
		h.logger.Info("server is running", log.Any("address", address))
		if h.cfg.Cert != "" && h.cfg.Key != "" && h.cfg.CA != "" {
			if err := h.server.ListenAndServeTLS(address, h.cfg.Cert, h.cfg.Key); err != nil {
				h.logger.Error("https server shutdown", log.Error(err))
			}
		} else {
			if err := h.server.ListenAndServe(address); err != nil {
				h.logger.Error("http server shutdown", log.Error(err))
			}
		}
	}()
}

func (h *HTTPServer) Close() {
	if h.server != nil {
		err := h.server.Shutdown()
		if err != nil {
			h.logger.Error("failed to shut down http server")
		}
	}
}
