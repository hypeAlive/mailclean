package de.alive.mailclean.infrastructure;

import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Mono;

public interface ConnectionManager {

    @NotNull Mono<IConnection> createConnection();
    @NotNull Mono<Boolean> reconnectConnection(@NotNull IConnection connection);
    @NotNull Mono<Boolean> closeConnection(@NotNull IConnection connection);
}
