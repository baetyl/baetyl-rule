package rule

import (
	"github.com/baetyl/baetyl-go/v2/context"
	"github.com/baetyl/baetyl-go/v2/errors"
	"github.com/baetyl/baetyl-go/v2/http"
	"github.com/baetyl/baetyl-go/v2/log"
	"github.com/baetyl/baetyl-go/v2/mqtt"
)

type ClientSet struct {
	clients map[string]*SingleClient //	key: client name
	server  *HTTPServer
}

type ClientDetail struct {
	Name         string
	Subscription []mqtt.QOSTopic
	Info         ClientInfo
}

func NewRulers(ctx context.Context, cfg Config, functionClient *http.Client) (*ClientSet, error) {
	var err error
	clientInfo := make(map[string]*ClientDetail) // key: client name, value: client config
	clientSet := &ClientSet{
		clients: make(map[string]*SingleClient),
	}
	for _, v := range cfg.Clients {
		if v.Kind == KindHTTPServer {
			// http server can only exist one
			if clientSet.server != nil {
				return nil, errors.New("Duplicate http source config")
			}
			clientSet.server, err = NewHTTPServer(v, functionClient)
			if err != nil {
				return nil, errors.Trace(err)
			}
			continue
		}
		clientInfo[v.Name] = &ClientDetail{
			Name: v.Name,
			Info: v,
		}
		clientSet.clients[v.Name] = &SingleClient{
			name:    v.Name,
			subTree: mqtt.NewTrie(),
			rulers:  make(map[string]RuleInfo), // key: rule name
			logger:  log.With(log.Any("client", v.Name)),
		}
	}

	for _, rule := range cfg.Rules {
		if rule.Target == nil {
			continue
		}
		// Set http source rule info
		if clientSet.server != nil && rule.Source.Client == clientSet.server.name {
			clientSet.server.rulers[rule.Name] = rule
			_, ok := clientInfo[rule.Target.Client]
			if !ok {
				return nil, errors.Trace(errors.Errorf("client (%s) not found in rule (%s)", rule.Target.Client, rule.Name))
			}
			continue
		}
		_, ok := clientInfo[rule.Source.Client]
		if !ok {
			return nil, errors.Trace(errors.Errorf("client (%s) not found in rule (%s)", rule.Source.Client, rule.Name))
		}
		if rule.Source.QOS > rule.Target.QOS {
			rule.Source.QOS = rule.Target.QOS
		}
		clientInfo[rule.Source.Client].Subscription = append(clientInfo[rule.Source.Client].Subscription, mqtt.QOSTopic{
			Topic: rule.Source.Topic,
			QOS:   uint32(rule.Source.QOS),
		})

		_, ok = clientInfo[rule.Target.Client]
		if !ok {
			return nil, errors.Trace(errors.Errorf("client (%s) not found in rule (%s)", rule.Target.Client, rule.Name))
		}
		singleClient, _ := clientSet.clients[rule.Source.Client]
		singleClient.rulers[rule.Name] = rule
		singleClient.subTree.Add(rule.Source.Topic, rule.Name)
	}

	// New all clients
	for _, v := range clientInfo {
		cli, err := NewClient(ctx, v)
		if err != nil {
			return nil, errors.Trace(err)
		}
		singleClient, _ := clientSet.clients[v.Name]
		singleClient.client = cli
	}
	// Start all clients
	for _, v := range clientInfo {
		singleClient, _ := clientSet.clients[v.Name]
		err = singleClient.Start(clientSet.clients, functionClient)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	// Start http server
	if clientSet.server != nil {
		for _, rule := range clientSet.server.rulers {
			clientSet.server.client[rule.Target.Client] = clientSet.clients[rule.Target.Client].client
		}
		clientSet.server.Start()
	}

	return clientSet, nil
}

func (l *ClientSet) Close() {
	for _, v := range l.clients {
		if v.client != nil {
			v.client.Close()
		}
	}
	if l.server != nil {
		l.server.Close()
	}
}
