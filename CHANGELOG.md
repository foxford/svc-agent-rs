# Changelog

## v0.14.2 (July 22, 2020)

### Changes
- Added `local_tracking_label` setters on outgoing properties ([1bd3f30](https://github.com/netology-group/svc-agent-rs/commit/1bd3f30cac66599bdec1e36c1d24b9f94711be33))

### Fixes
- Fixed reconnection not sending AgentNotification::Reconnection message ([49b853f](https://github.com/netology-group/svc-agent-rs/commit/49b853f03994903553e97cce8b24362d27014672))


## v0.14.1 (June 8, 2020)

### Changes
- MQTT notifications loop thread is now named for ease of debugging ([5ab4e13](https://github.com/netology-group/svc-agent-rs/commit/5ab4e13c010d3e1d5efda22dadd13a1e9717bb91))

### Fixes
- Updated rumq with git version so it now accounts for max_packet_size option ([b3cef19](https://github.com/netology-group/svc-agent-rs/commit/b3cef19cf320114553d4e20ede7ebd3feb026a20))

## v0.14.0 (June 4, 2020)

### Breaking changes
- Switch MQTT libraries from rumqtt to rumq ([8c20c3c](https://github.com/netology-group/svc-agent-rs/commit/8c20c3c34e468982458d7efb85a2b154aca0e755))

## v0.13.1 (May 28, 2020)

### Breaking changes

- Added Publish metadata to `AgentNotification::Message()` enum variant (now its `AgentNotification::Message(message, metadata)`) ([30a068c](https://github.com/netology-group/svc-agent-rs/commit/30a068c84342bf7d5c3dd7613375db241cde2858))

### Features

- Added `set_agent_id(AgentId)` method for `OutgoingEventProperties` ([f43580e](https://github.com/netology-group/svc-agent-rs/commit/f43580ea78a38c5753f3a227d00834b160ccae99))


## v0.13.0 (May 26, 2020)

### Breaking changes

- Reworked IncomingMessage and OutgoingMessage as enums ([96f3ab0](https://github.com/netology-group/svc-agent-rs/commit/96f3ab053bf3a8a39c15687cbaf3c20f0d9606ad))

### Features

- Added queue-counter feature ([d17357e](https://github.com/netology-group/svc-agent-rs/commit/d17357e494b0901d8f659aa8c836a850756c5771))

## v0.12.1 (April 24, 2020)

### Features

- Multicast events ([d70d3963](https://github.com/netology-group/svc-agent-rs/commit/d70d3963f3b19be9aa83b02e258e743fa59a37da) and [fae57b9](https://github.com/netology-group/svc-agent-rs/commit/fae57b9464ceea5f48f1efc60ce4f5760eb25803))

## v0.12.0 (April 21, 2020)

### Breaking changes
- Changed default value of AgentConfig::reconnect_interval from 10 seconds to never reconnect ([f02869f](https://github.com/netology-group/svc-agent-rs/commit/f02869f3f7cdd44074203715f232684a57d691b7))

## v0.11.4 (April 17, 2020)

### Fixes
- Skip serializing empty timestamp ([785a657](https://github.com/netology-group/svc-agent-rs/commit/785a657c567896403b0df3cfff072e40e084d1a7))


## v0.11.3 (April 17, 2020)

### Features
- Split up incoming & outgoing short term timings ([0acd988](https://github.com/netology-group/svc-agent-rs/commit/0acd9883281f714fd865db322a64e0014f385526))


## v0.11.2 (March 3, 2020)

### Features
- Add `max_message_size` config option ([5a820c4](https://github.com/netology-group/svc-agent-rs/commit/5a820c4596a544209a2d0e597c5b96565020371d))
- Add `OutgoingRequestProperties::set_local_timestamp` ([721ccf54](https://github.com/netology-group/svc-agent-rs/commit/721ccf54004301da0a4302bc900773fdc8230e02))


## v0.11.1 (February 6, 2020)

### Features
- Add request dispatcher ([c6d7191](https://github.com/netology-group/svc-agent-rs/commit/c6d71916430073268fa25e843d9ef74e482e3b93))
- Make all incoming properties serializable ([3d033d8](https://github.com/netology-group/svc-agent-rs/commit/3d033d8e2ddfbd17e4759708cb8be5a45b208f05))
- Allow creating a subscription to an arbitrary topic  ([90de181](https://github.com/netology-group/svc-agent-rs/commit/90de18160daac56ebff2d6033ea37d8bfc4d4f8b))


## v0.11.0 (January 21, 2020)

### Changes
- Overhaul Publishable and related traits ([79c06e5](https://github.com/netology-group/svc-agent-rs/commit/79c06e569e76f44fcfd581b171c84c147c319651), [7d682f0](https://github.com/netology-group/svc-agent-rs/commit/7d682f048c504ac67342fbbe4cbcafe380058419))

### Fixes
- Skip empty agent_id serialization ([af2f2fe](https://github.com/netology-group/svc-agent-rs/commit/af2f2fe21c528c75f85b73e4a3023b3c7ae4dba1))


## v0.10.0 (December 20, 2019)

### Features
- Upgrade to v2 connection ([991f2b4](https://github.com/netology-group/svc-agent-rs/commit/991f2b484ea0082a5fee770f3ebd0dd4ddc870ef))
- Parametrize version in topics ([6feb43b](https://github.com/netology-group/svc-agent-rs/commit/6feb43b12c8d26d62f7de4978b468483d7807a5f))
- Separate API version from connection version ([49fc797](https://github.com/netology-group/svc-agent-rs/commit/49fc797649e8057f56045715f256804e45b0ffe8))
- Derive `Clone` & `Serialize` for `IncomingEventProperties` ([da9c574](https://github.com/netology-group/svc-agent-rs/commit/da9c5749bb9e07f3b08a6a5a9852c9043bef30c6))


## v0.9.5 (November 21, 2019)

### Fixes
- Skip serializing empty local tracking label ([42a51c4](https://github.com/netology-group/svc-agent-rs/commit/42a51c4993fad0c54c08986a6d7855e4bd96812c))

## v0.9.5 (November 21, 2019)

### Fixes
- Fix tracking id ([2150339](https://github.com/netology-group/svc-agent-rs/commit/2150339a4645fd5eb171588bf555f2e7be175603))


## v0.9.4 (November 21, 2019)

### Features
- Add tracking ([2fe8b8c](https://github.com/netology-group/svc-agent-rs/commit/2fe8b8c6b5c1cfd520232d7c7db1810f7949284d))


## v0.9.3 (November 12, 2019)

### Fixes
- Support negative durations ([cff7b99](https://github.com/netology-group/svc-agent-rs/commit/cff7b990247f6ff948261201562515888d32b32b))


## v0.9.2 (November 8, 2019)

### Fixes
- Skip serializing empty timing properties ([fbf4662](https://github.com/netology-group/svc-agent-rs/commit/fbf4662ae234457f56fef35df4aeea0e45d85d4b))


## v0.9.1 (November 1, 2019)

### Fixes
- Serialize timestamps & durations as strings ([0a647f4](https://github.com/netology-group/svc-agent-rs/commit/0a647f4d1c118cca3c4d8270d9ca49377b08b336))


## v0.9.0 (October 29, 2019)

### Features
- Add timing ([b338621](https://github.com/netology-group/svc-agent-rs/commit/b338621aa355e98aaaa34c6352a24e5127d81e35))


## v0.8.5 (October 2, 2019)

### Features
- Update rumqtt version to 0.31 ([c3d02ea](https://github.com/netology-group/svc-agent-rs/commit/c3d02ea0d6fc448c135c5cf978c156f68aa2615c))


## v0.8.4 (September 30, 2019)

### Changes
- Add broker properties to `IncomingRequest` ([ad40e63](https://github.com/netology-group/svc-agent-rs/commit/ad40e63956178a1a2218115a00d42237abd820d6))


## v0.8.3 (September 20, 2019)

### Fixes
- Skip response topic (de)serialization ([b057a28](https://github.com/netology-group/svc-agent-rs/commit/b057a281f0a66f9e77a95a0ffd8e9e7b1d1f94e2))



## v0.8.2 (September 19, 2019)

### Changes
- Add getters & deserialization for Connection ([36cb194](https://github.com/netology-group/svc-agent-rs/commit/36cb194d7a859d73153ca5980b8e94d5e929c393))
- Send response to request's response topic ([9a1cf6f](https://github.com/netology-group/svc-agent-rs/commit/9a1cf6fcb714e36d38e9c3af40fb42186f4e0de4))
- Make incoming event's label optional ([20f93f7](https://github.com/netology-group/svc-agent-rs/commit/20f93f7cbf8b9436e6def3ea3302bc96dd708650))



## v0.8.1 (September 12, 2019)

### Features
- Add `label` to `IncomingEvent` ([2c454c2](https://github.com/netology-group/svc-agent-rs/commit/2c454c269d7429fc8f580c13473b3d4c8a71764b))



## v0.8.0 (September 10, 2019)

### Changes
- Implement `Publishable` on `OutgoingMessage`, remove `Publish` trait ([4968cdf](https://github.com/netology-group/svc-agent-rs/commit/4968cdfb338b17385129ffef8ec421eb1c7b9e56))
- Implement `DestinationTopic` on `AgentId` ([04020b0](https://github.com/netology-group/svc-agent-rs/commit/04020b04bfef0421e8b7785054a4cbf4bf0ca86f))
- Remove `response_topic` field from `OutgoingResponse` ([c797b44](https://github.com/netology-group/svc-agent-rs/commit/c797b44e007978cc47dd999ec966a63b9cd8b797))
- Replace `to_bytes` with `into_bytes` in `Publishable` trait ([9b0df11](https://github.com/netology-group/svc-agent-rs/commit/9b0df11f6e10ce7dc09e2135cc45bb5f0dc50ed7))



## v0.7.3 (August 14, 2019)

### Changes
- Serialize `status` as string for MQTT v5 user properties compatibility ([3fb1d31](https://github.com/netology-group/svc-agent-rs/commit/3fb1d31c69387eb34f6e748fb221c684272e929a))



## v0.7.2 (June 28, 2019)

### Features
- Add connection type and incoming message properties ([d65bda8](https://github.com/netology-group/svc-agent-rs/commit/d65bda8135d352d52d55a45cf87be6cbf73cfa79))
- Add support for observer mode ([65f12fa](https://github.com/netology-group/svc-agent-rs/commit/65f12faf2cade018a06ab61f8252c16a9943a571))



## v0.7.1 (June 5, 2019)

### Features
- Add password and username to the configuration ([0d80f45](https://github.com/netology-group/svc-agent-rs/commit/0d80f45b031fcfc615111fe35495a56b7356ce9f))



## v0.7.0 (June 3, 2019)

### Changes
- Change default connection version to `v1` ([1a6eb4f](https://github.com/netology-group/svc-agent-rs/commit/1a6eb4f0adca9212daf29b7283cfb6267d7cffc6))



## v0.6.1 (May 18, 2019)

### Features
-  Add `properties` function to `OutgoingMessage` ([0a9d445](https://github.com/netology-group/svc-agent-rs/commit/0a9d445ba5eaadb740907583e5d5b35059ad3cd8))



## v0.6.0 (May 7, 2019)

### Changes
-  Use `QoS::AtMostOnce` for requests and `QoS::AtLeastOnce` for responses and events ([61834f0](https://github.com/netology-group/svc-agent-rs/commit/61834f0d7d41b882dd1ad1350c695a5b1ba7ad3e))



## v0.5.1 (Apr 24, 2019)

### Features
- Add `keep_alive_interval`, `reconnect_interval`, `outgoing_message_queue_size`, and `incoming_message_queue_size` configuration options ([3ba5a4d](https://github.com/netology-group/svc-agent-rs/commit/3ba5a4dfafbe7ae2326dfa7fda1b3a1802161c5c))

### Changes
- Make `clean_session` configuration option optional ([3ba5a4d](https://github.com/netology-group/svc-agent-rs/commit/3ba5a4dfafbe7ae2326dfa7fda1b3a1802161c5c))



## v0.5.0 (Apr 18, 2019)

### Features
- Add additional subscription functions ([3a2f2ab5](https://github.com/netology-group/svc-agent-rs/commit/3a2f2ab5e943519b5c9e4f6c7aaa2551c58d1fbb))

### Changes
- Replace `OutgoingResponseStatus` with `ResponseStatus` ([a6453f](https://github.com/netology-group/svc-agent-rs/commit/a6453f0222d95b23429d39a175d6d62190ee34ec))



## v0.4.1 (Apr 17, 2019)

### Features
- Implement `Deserialize` for `OutgoingResponseStatus` ([296173b](https://github.com/netology-group/svc-agent-rs/commit/296173b815d4ecdaa1a3e83dc2e89704cd9d65cf))



## v0.4.0 (Apr 5, 2019)

### Features
- Add `Error` type ([f76c694](https://github.com/netology-group/svc-agent-rs/commit/f76c694b97ab3aeeac7186886a88720f58c34461))
- Implement `Clone` for `Agent` ([511d01c](https://github.com/netology-group/svc-agent-rs/commit/511d01c14e6d7ee839165e0cb4f7e74ad65f9f4c))



## v0.3.1 (Mar 5, 2019)

### Features
- Add `clean_session` configuration option ([5efa53c](https://github.com/netology-group/svc-agent-rs/commit/5efa53c65ab520c61352ed44122c5d7c6aa62a5b))



## v0.3.0 (Mar 1, 2019)

### Features
- Implement `Eq`, `Hash`, `Serialize` and `Deserialize` for `AgentId` and `SharedGroup` ([4a5fe76](https://github.com/netology-group/svc-agent-rs/commit/4a5fe76dbe8c269635998070a7c742aa41595570))
- Add `Service` connection mode

### Changes
- Replace `Agent` connection mode with `Default`



## v0.2.0 (Feb 14, 2019)

### Features
- Add `Addressable` trait ([39d5faa](https://github.com/netology-group/svc-agent-rs/commit/39d5faad19f6209b88beface622c273e90dcd9c7))

### Changes
- Change arguments of `OutgoingRequestProperties` constructor ([cba56a9](https://github.com/netology-group/svc-agent-rs/commit/cba56a98f172a111042db9e544f8bd6762217f3e))
- Replace `agent_id` with `as_agent_id` ([39d5faa](https://github.com/netology-group/svc-agent-rs/commit/39d5faad19f6209b88beface622c273e90dcd9c7))



## v0.1.0 (Feb 12, 2019)

Initial release
