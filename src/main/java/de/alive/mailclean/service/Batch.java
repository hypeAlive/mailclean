package de.alive.mailclean.service;

import de.alive.mailclean.infrastructure.IConnection;
import reactor.core.publisher.Flux;

public interface Batch {

    Flux<BatchEntity> entities(IConnection connection);

}
