package de.alive.mailclean.domain;

import de.alive.mailclean.service.BatchEntity;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class Email implements BatchEntity {
    private final int messageNumber;
    private final String subject;
    private final ProcessFolder folder;
}