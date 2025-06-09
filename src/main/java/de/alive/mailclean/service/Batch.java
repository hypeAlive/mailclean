package de.alive.mailclean.service;

import de.alive.mailclean.infrastructure.IConnection;

import javax.mail.MessagingException;
import java.util.List;

public interface Batch {

    List<BatchEntity> entities(IConnection connection);

}
