package de.alive.mailclean.service;

import de.alive.mailclean.domain.Email;

public interface EmailAnalysisService {
    void analyzeEmail(Email email);
}