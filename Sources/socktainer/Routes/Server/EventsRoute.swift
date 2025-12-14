import ContainerClient
import NIOCore
import Vapor

struct EventsRoute: RouteCollection {
    let client: ClientHealthCheckProtocol
    func boot(routes: RoutesBuilder) throws {
        try routes.registerVersionedRoute(.GET, pattern: "/events", use: EventsRoute.handler(client: client))
    }

}

extension EventsRoute {
    static func handler(client: ClientHealthCheckProtocol) -> @Sendable (Request) async throws -> Response {
        { req in

            let broadcaster = req.application.storage[EventBroadcasterKey.self]!
            let stream = await broadcaster.stream()

            let response = Response(status: .ok)
            response.headers.add(name: .contentType, value: "application/json")

            response.body = .init(stream: { writer in
                actor StopSignal {
                    var shouldStop = false
                    func stop() { shouldStop = true }
                    func check() -> Bool { shouldStop }
                }
                let stopSignal = StopSignal()
                Task {
                    for await event in stream {
                        // if Task.isCancelled { break }
                        if await stopSignal.check() { break }
                        guard let json = try? JSONEncoder().encode(event) else { continue }
                        var buffer = req.application.allocator.buffer(capacity: json.count + 1)
                        buffer.writeBytes(json)
                        buffer.writeString("\n")
                        writer.write(.buffer(buffer)).whenFailure { error in
                            switch error {
                            case is IOError:
                                req.logger.info("Client disconnected (broken pipe)")
                                Task { await stopSignal.stop() }
                            case let error as ChannelError where error == .ioOnClosedChannel:
                                req.logger.info("Client disconnected (closed channel)")
                                Task { await stopSignal.stop() }
                            default:
                                // NOTE: Consider improving logging
                                req.logger.warning("\(event) raised '\(error)'")
                            }
                        }
                            /*
                            do {
                                try await writer.write(.buffer(buffer)).get()
                            } catch let error as IOError {
                                req.logger.info("Client disconnected (broken pipe)")
                                break
                            } catch let error as ChannelError where error == .ioOnClosedChannel {
                                req.logger.info("Client disconnected (closed channel)")
                                break
                            } catch {
                                // NOTE: Consider improving logging
                                req.logger.warning("\(event) raised '\(error)'")
                            }
                            */
                    }
                }
            })

            return response

        }
    }
}
