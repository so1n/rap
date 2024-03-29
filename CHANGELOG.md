### 0.5.5[TODO]
 - Feature: add server config

### 0.5.4[TODO]
 - description: near-continuous availability and a high degree of reliability.
 - Feature: client support HA strategy
 - Feature: client support load balance
 - Feature: server HA middleware

### 0.5.3.8[Now]
 - description:
 - Feature: add channel future
 - Feature: client support inject func
 - Feature: add opentracing processor
 - Feature: add server api gateway
 - Fix: fix client session run rap func bug
 - Optimize: optimize common and server code

### 0.5.3.7
 - description:
 - Feature: add limit processor
 - Feature: Channel support group
 - Feature: func type check
 - Feature: support kwargs param
 - Fix: Client session.execute not execute coroutine bug
 - Fix: can not register asyncgen func
 - Refactor: server response api
 - Refactor: improvements 'as_first_complete' func and transport result_future
 - Remove: remove redis depend by server
 - Test: add test and support coverage 90%+
 - Style: add mypy

### 0.5.3.6
 - Fix: client crypto memory bug
 - Feature: server support register private func
 - Feature: client register support alias & group
 - Refactor: server start event&stop event
 - Refactor: protocol's event flag & header column & error response

### 0.5.3.5
 - Remove: remove life cycle
 - Feature: add transport result future(listen transport exc)
 - Feature: read transport data add listen transport exc
 - Feature: print cli func list table
 - Feature: channel add method `iter_body` & `iter`
 - Refactor: refactor client session and add session execute
 - Refactor: func manager call func

### 0.5.3.4
 - description: Add code comments; fix bugs; simplify the protocol
 - Doc: add code comments
 - Fix: fix server channel crypto&future cancel bug
 - Feature: add load event
 - Feature: support mutli server
 - Feature: add transport sock name and modify client listen future id
 - Refactor: refactor func manager
 - Refactor: refactor server crypto nonce check

### 0.5.3.3
 - description: support session & channel
 - Feature: add use_session column in header
 - Feature: add request_id column in header
 - Feature: add channel
 - Feature: add processor
 - Feature: client:AsyncIteratorCall support client session
 - Refactor: func manager
 - Refactor: middleware
 - Refactor: rap protocol

### 0.5.3.2
 - description: simplify&mutli server
 - Feature: remove life cycle
 - Fix: keepalive time init error
 - Refactor: refactor server
 - Refactor: refactor client, support transport mutli server
 - Remove: remove server client_model(one transport one request object)

### 0.5.3.1
 - description: request interface
 - Refactor: request_handle&core interface

### 0.5.3
 - description: new middleware design&redis support
 - Feature: server&client remove aes
 - Feature: client add middleware feat
 - Feature: server&client add aes middleware
 - Feature: add server.response middleware
 - Feature: middleware add redis support
 - Feature: Ping&Pong, transport keep alive
 - Refactor: server.load_middleware

### 0.5.2.2
 - description: Distinguish server exceptions and execution function exceptions; better log output
 - Refactor: Exceptions
 - Feature: Complete server exception
 - Feature: Complete execution func exception
 - Refactor: log output

### 0.5.2.1
 - description: hotfix
 - Fix: server transport read recv data bug
 - Fix: crypto error when client reconnect
 - Fix: When the log level is not debug, the debug information will be calculated

### 0.5.2
 - description: Faster&Humanize
 - Feature: client&server support transport multiplexing(Faster~)
 - Fix: read recv data bug
 - Refactor: remove connection pool and connection lock

### 0.5.1
 - description: Improve rpc protocol
 - Fix: Aes bug
 - Fix: declare life cycle response not timeout&nonce param
 - Feature: Server check register func return param&param  annotation
 - Feature: support version
 - Refactor: server response
 - Refactor: server middleware
 - Refactor: exceptions

## 0.5
 - description: Use more flexible rpc protocol
 - Refactor: new protocol

### 0.3.2
 - Feature: add ip limit transport middleware
 - Refactor: split RequestHandle to request and response

### 0.3.1
 - Feature: load and reload fun in running
 - Feature: add local client and support rap call _root_fun

## 0.3
 - Feature: support aes

### 0.2.2
 - Refactor: modify server iterator design

### 0.2.1
 - Fix: one start concurrent bug

## 0.2
 - Feature: support pool

### 0.1.5
 - Feature: support python decorator

## 0.1
 - description: The first version
 - Feature: support python rpc
