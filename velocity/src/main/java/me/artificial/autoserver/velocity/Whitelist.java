package me.artificial.autoserver.velocity;

import com.velocitypowered.api.proxy.Player;
import org.slf4j.Logger;

import java.io.*;
import java.nio.file.*;
import java.util.*;

public class Whitelist {
    private final Path whitelistFile;
    private final Set<UUID> whitelistedUUIDs = new HashSet<>();
    private boolean enabled = false;
    private WatchService watchService;
    private Thread watchThread;
    private final Logger logger;

    public Whitelist(Path dataDirectory, Logger logger) {
        this.whitelistFile = dataDirectory.resolve("whitelist.txt");
        this.logger = logger;
    }

    public void load() throws IOException {
        // create file if missing
        if (!Files.exists(whitelistFile)) {
            Files.createDirectories(whitelistFile.getParent());
            Files.createFile(whitelistFile);
        }

        reload();
    }

    private void reload() throws IOException {
        Set<UUID> newUUIDs = new HashSet<>();
        boolean newEnabled = false;

        List<String> lines = Files.readAllLines(whitelistFile);
        for (String line : lines) {
            line = line.trim();
            if (line.isEmpty() || line.startsWith("#")) continue;
            if (line.equalsIgnoreCase("enabled=true")) {
                newEnabled = true;
                continue;
            }
            if (line.equalsIgnoreCase("enabled=false")) {
                newEnabled = false;
                continue;
            }
            try {
                newUUIDs.add(UUID.fromString(line));
            } catch (IllegalArgumentException ignored) {
                // skip invalid lines
            }
        }

        synchronized (this) {
            whitelistedUUIDs.clear();
            whitelistedUUIDs.addAll(newUUIDs);
            enabled = newEnabled;
        }

        logger.info("Whitelist reloaded: {} UUIDs, enabled={}", whitelistedUUIDs.size(), enabled);
    }

    public void startWatcher() {
        try {
            watchService = FileSystems.getDefault().newWatchService();
            whitelistFile.getParent().register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);

            watchThread = new Thread(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        WatchKey key = watchService.take();
                        for (WatchEvent<?> event : key.pollEvents()) {
                            Path changed = (Path) event.context();
                            if (changed.getFileName().toString().equals("whitelist.txt")) {
                                Thread.sleep(100); // small delay to ensure write is complete
                                reload();
                            }
                        }
                        key.reset();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (IOException e) {
                        logger.warn("Error reloading whitelist: {}", e.getMessage());
                    } catch (ClosedWatchServiceException e) {
                        break;
                    }
                }
            }, "autoserver-whitelist-watcher");
            watchThread.setDaemon(true);
            watchThread.start();
            logger.info("Whitelist file watcher started.");
        } catch (IOException e) {
            logger.warn("Failed to start whitelist watcher: {}", e.getMessage());
        }
    }

    public void stopWatcher() {
        if (watchThread != null) watchThread.interrupt();
        if (watchService != null) {
            try { watchService.close(); } catch (IOException ignored) {}
        }
    }

    public synchronized boolean isEnabled() {
        return enabled;
    }

    public synchronized boolean isWhitelisted(Player player) {
        return whitelistedUUIDs.contains(player.getUniqueId());
    }
}
