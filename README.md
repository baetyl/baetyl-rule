baetyl-rule
========

[![Build Status](https://travis-ci.org/baetyl/baetyl-rule.svg?branch=master)](https://travis-ci.org/baetyl/baetyl-rule)
[![Go Report Card](https://goreportcard.com/badge/github.com/baetyl/baetyl-rule)](https://goreportcard.com/report/github.com/baetyl/baetyl-rule) 
[![codecov](https://codecov.io/gh/baetyl/baetyl-rule/branch/master/graph/badge.svg)](https://codecov.io/gh/baetyl/baetyl-rule)
[![License](https://img.shields.io/github/license/baetyl/baetyl-rule.svg)](./LICENSE)

# 简介

Baetyl-Rule 可以实现 Baetyl 框架端侧的消息流转，在 [Baetyl-Broker](https://github.com/baetyl/baetyl-broker)(端侧消息中心)、函数服务、Iot Hub(云端 Mqtt Broker) 进行消息交换。

支持以下的消息流转方式：
- 订阅来自mqtt的消息，开放openapi，接受来自http的消息。
- 调用函数计算（此步骤可以省略）
- 将处理后的结果发送至mqtt或调用http服务

其中 Baetyl 支持 Python、Node、Sql 等多种运行时，可以配置相关的脚本函数对消息进行过滤、处理、转换以及丰富等，具体可以参考 [Baetyl-Function](https://github.com/baetyl/baetyl-function) 模块。

Baetyl-Rule 的全量配置文件如下，并对配置字段做了相应解释：

```yaml
clients: # 消息节点，可以从消息节点订阅消息，也可以发送至消息节点
  - name: iothub # 名称
    kind: mqtt # mqtt 类型
    address: 'ssl://u7isgiz.mqtt.iot.bj.baidubce.com:1884' # 地址
    username: client.example.org # 用户名
    ca: ../example/var/lib/baetyl/testcert/ca.pem # 连接节点的 CA
    key: ../example/var/lib/baetyl/testcert/client.key # 连接节点的私钥
    cert: ../example/var/lib/baetyl/testcert/client.pem # 连接节点的公钥
    insecureSkipVerify: true # 是否跳过服务端证书校验
  - name: http-client # 名称
    kind: http        # http类型(仅可配置为target)
    address: 'http://127.0.0.1:8554'  # http服务地址，如果是https，请配置证书，否则默认使用系统证书
    ca: ../example/var/lib/baetyl/testcert/ca.pem # 连接节点的 CA
    key: ../example/var/lib/baetyl/testcert/client.key # 连接节点的私钥
    cert: ../example/var/lib/baetyl/testcert/client.pem # 连接节点的公钥
    insecureSkipVerify: true # 是否跳过服务端证书校验
  - name: http-server      # 名称
    kind: http-server      # http-server类型（仅可配置为source）
    port: 8090             # http server服务端口，下面的tls可选，不配置时，默认为http服务
    ca: ../example/var/lib/baetyl/testcert/ca.pem # 服务器的 CA
    key: ../example/var/lib/baetyl/testcert/client.key # 服务器的私钥
    cert: ../example/var/lib/baetyl/testcert/client.pem # 服务器的公钥
    insecureSkipVerify: true # 是否跳过服务端证书校验
rules:                     # 消息规则
  - name: rule1            # 规则名称，必须保持唯一
    source:                # 消息源
      topic: broker/topic1 # 消息主题
      qos: 1               # 消息质量
    target:                # 消息目的地
      client: iothub       # 消息节点，如果不设置，默认为 baetyl-broker
      topic: iothub/topic2 # 消息主题
      qos: 0               # 消息质量
    function:              # 处理函数
      name: node85         # 函数名称
  - name: rule2            # 规则名称，必须保持唯一
    source:                # 消息源
      topic: broker/topic5 # 消息主题
      qos: 0               # 消息质量
    target:                # 消息目的地
      client: http-client  # 与clients中配置的http服务名称一致
      path: /nodes/test    # http访问路径
      method: PUT          # http消息类型，支持GET/POST/PUT/DELETE
  - name: rule3            # 规则名称，必须保持唯一
    source:                # 消息源
      client: http-server  # 指定为http-server的消息源
    target:                # 消息目的地
      client: http-client  # 与clients中配置的http服务名称一致
      path: /nodes/test    # http访问路径
      method: PUT          # http消息类型，支持GET/POST/PUT/DELETE
```

说明：

- baetyl-rule 后台默认添加边缘系统应用baetyl-broker作为一个消息节点
- 当一条rule规则的source/target 未配置 client 字段时，会默认使用 baetyl-broker 作为其消息节点
- 当一个client消息节点的kind为http 时，若address 连接地址使用https，默认使用baetyl-core签发的系统证书
- http类型消息节点只能作为一条rule的target，并且http请求的Content-Type为application/json
- http-server类型仅可作为rule的source，且该类型仅可存在一个配置，用户调用时，使用POST请求访问地址`http://{ip}:{port}/rules/{ruleName}` 来触发调用

## Demo示例

### 消息流转+函数计算

下面示例定义了三条规则:

- rule1：订阅broker/topic1消息，将消息作为函数`py-demo1/func1`的输入，将函数计算结果输出至`broker/topic2`
- rule2：订阅broker/topic3消息，将消息作为函数`py-demo1/func2`的输入，将函数计算结果通过http POST请求发送至`http://127.0.0.1:8554/rule/result`

```yaml
clients:
  - name: http-server
    kind: http
    address: 'http://127.0.0.1:8554'
rules:
  - name: rule1
    source:
      topic: broker/topic1
    target:
      topic: broker/topic2
    function:
      name: py-demo1/func1
  - name: rule2
    source:
      topic: broker/topic3
    target:
      client: http-server
      path: /rule/result
      method: POST
    function:
      name: py-demo1/func2
logger:
  level: debug
  encoding: console
```
