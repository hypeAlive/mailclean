package de.alive.mailclean.service;

import de.alive.mailclean.domain.FolderInfo;
import de.alive.mailclean.infrastructure.ImapConnectionManager;
import lombok.extern.slf4j.Slf4j;

import javax.mail.Folder;
import javax.mail.Store;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class FolderDiscoveryService {

    private final ImapConnectionManager connectionManager;

    public FolderDiscoveryService(ImapConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    public List<FolderInfo> discoverAllFolders() {
        List<FolderInfo> folderInfos = new ArrayList<>();

        try {
            var connection = connectionManager.createConnection(0, 0);
            if (connection == null) {
                log.error("Cannot create connection for folder discovery");
                return folderInfos;
            }

            Store store = connection.getStore();
            Folder[] folders = store.getDefaultFolder().list("*");

            for (Folder folder : folders) {
                try {
                    folder.open(Folder.READ_ONLY);
                    int messageCount = folder.getMessageCount();

                    if (messageCount > 0) {
                        folderInfos.add(new FolderInfo(folder.getFullName(), messageCount));
                        log.info("📁 Folder '{}': {} messages", folder.getFullName(), messageCount);
                    }

                    folder.close();
                } catch (Exception e) {
                    log.warn("⚠️ Could not analyze folder '{}': {}", folder.getFullName(), e.getMessage());
                }
            }

        } catch (Exception e) {
            log.error("Error discovering folders", e);
        }

        return folderInfos;
    }
}