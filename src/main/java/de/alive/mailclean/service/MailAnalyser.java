package de.alive.mailclean.service;

import de.alive.mailclean.domain.Email;

public interface MailAnalyser {
    void analyzeMail(Email email);
}