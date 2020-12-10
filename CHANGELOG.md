### 0.5.5
 - Feature: add server config
 
### 0.5.4
 - description: near-continuous availability and a high degree of reliability.
 - Feature: client support HA strategy
 - Feature: client support load balance
 - Feature: server HA middleware

### 0.5.3.1[now]
 - description: request interface 
 - Refactor: request_handle&core interface

### 0.5.3
 - description: new middleware design&redis support
 - Feat: server&client remove aes
 - Feat: client add middleware feat
 - Feat: server&client add aes middleware
 - Feat: add server.response middleware
 - Feat: middleware add redis support
 - Feat: Ping&Pong, conn keep alive 
 - Refactor: server.load_middleware

### 0.5.2.2
 - description: Distinguish server exceptions and execution function exceptions; better log output
 - Refactor: Exceptions
 - Feat: Complete server exception
 - Feat: Complete execution func exception
 - Refactor: log output
 
### 0.5.2.1
 - description: hotfix
 - Fix: server conn read recv data bug
 - Fix: crypto error when client reconnect 
 - Fix: When the log level is not debug, the debug information will be calculated

### 0.5.2
 - description: Faster&Humanize
 - Feature: client&server support conn multiplexing(Faster~)
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

### 0.5
 - description: Use more flexible rpc protocol
 - Refactor: new protocol

### 0.3.2
 - Feature: add ip limit conn middleware
 - Refactor: split RequestHandle to request and response

### 0.3.1
 - Feature: load and reload fun in running
 - Feature: add local client and support rap call _root_fun

## 0.3
 - Feature: support aes

### 0.2.2 
 - Refactor: modify server iterator design

### 0.2.1 
 - Fix: one connect concurrent bug

## 0.2
 - Feature: support pool

### 0.1.5
 - Feature: support python decorator

## 0.1
 - description: The first version
 - Feature: support python rpc
 
