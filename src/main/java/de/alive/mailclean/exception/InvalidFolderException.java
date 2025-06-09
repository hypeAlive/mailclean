package de.alive.mailclean.exception;

import de.alive.mailclean.domain.ProcessFolder;
import org.jetbrains.annotations.NotNull;

import javax.mail.Folder;

public class InvalidFolderException extends RuntimeException {

    private InvalidFolderException(@NotNull String folderName, @NotNull String reason) {
        super("Invalid folder '" + folderName + "': " + reason);
    }

    @NotNull
    public static InvalidFolderException emptyName(@NotNull ProcessFolder folder) {
        return new InvalidFolderException(folder.getFolder().getFullName(), "cannot be empty or whitespace only");
    }

    @NotNull
    public static InvalidFolderException negativeMsgCount(@NotNull Folder folder, int messageCount) {
        return new InvalidFolderException(folder.getFullName(),
                String.format("message count cannot be negative (%d)", messageCount)
        );
    }

    @NotNull
    public static InvalidFolderException invalidBatchRange(@NotNull ProcessFolder.MailBatch batch) {
        return new InvalidFolderException(
                String.format("batch (folder: %s)", batch.folder().getFolder().getFullName()),
                String.format("invalid range: start=%d, end=%d", batch.start(), batch.end())
        );
    }
}