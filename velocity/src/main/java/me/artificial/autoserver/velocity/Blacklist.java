package me.artificial.autoserver.velocity;

import com.velocitypowered.api.proxy.Player;
import com.velocitypowered.api.proxy.ProxyServer;
import net.kyori.adventure.text.minimessage.MiniMessage;
import org.slf4j.Logger;

import java.io.*;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class Blacklist {
    private final Path blacklistFile;
    private final Logger logger;
    private final ProxyServer proxy;
    private final List<BlacklistEntry> entries = new ArrayList<>();
    private WatchService watchService;
    private Thread watchThread;

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

    public Blacklist(Path dataDirectory, Logger logger, ProxyServer proxy) {
        this.blacklistFile = dataDirectory.resolve("blacklist.txt");
        this.logger = logger;
        this.proxy = proxy;
    }

    public static class BlacklistEntry {
        public final String target;       // username or IP
        public final String reason;
        public final LocalDateTime expiry; // null = permanent

        public BlacklistEntry(String target, String reason, LocalDateTime expiry) {
            this.target = target.toLowerCase();
            this.reason = reason;
            this.expiry = expiry;
        }

        public boolean isExpired() {
            return expiry != null && LocalDateTime.now().isAfter(expiry);
        }

        public String expiryString() {
            return expiry == null ? "Never" : expiry.format(FORMATTER);
        }
    }

    public void load() throws IOException {
        if (!Files.exists(blacklistFile)) {
            Files.createDirectories(blacklistFile.getParent());
            // write default example file
            Files.writeString(blacklistFile,
                "# Blacklist file\n" +
                "# Format: target | reason | expiry\n" +
                "# target: username or IP address\n" +
                "# reason: any text\n" +
                "# expiry: yyyy-MM-dd HH:mm or 'permanent'\n" +
                "# Example:\n" +
                "# BadPlayer | Cheating | 2026-12-31 00:00\n" +
                "# 1.2.3.4 | Spam | permanent\n"
            );
        }
        reload();
    }

    private void reload() throws IOException {
        List<BlacklistEntry> newEntries = new ArrayList<>();
        List<String> lines = Files.readAllLines(blacklistFile);

        for (String line : lines) {
            line = line.trim();
            if (line.isEmpty() || line.startsWith("#")) continue;

            String[] parts = line.split("\\|");
            if (parts.length < 2) continue;

            String target = parts[0].trim();
            String reason = parts[1].trim();
            LocalDateTime expiry = null;

            if (parts.length >= 3) {
                String expiryStr = parts[2].trim();
                if (!expiryStr.equalsIgnoreCase("permanent")) {
                    try {
                        expiry = LocalDateTime.parse(expiryStr, FORMATTER);
                    } catch (Exception e) {
                        logger.warn("Invalid expiry date for blacklist entry '{}': {}", target, expiryStr);
                    }
                }
            }

            newEntries.add(new BlacklistEntry(target, reason, expiry));
        }

        synchronized (this) {
            entries.clear();
            entries.addAll(newEntries);
        }

        logger.info("Blacklist reloaded: {} entries", entries.size());
        kickIfBlacklisted();
    }

    private void kickIfBlacklisted() {
        for (Player player : proxy.getAllPlayers()) {
            BlacklistEntry entry = getBlacklistEntry(player);
            if (entry != null) {
                player.disconnect(MiniMessage.miniMessage().deserialize(
                    "<red><bold>You are blacklisted!</bold></red>\n<white>Reason: " + entry.reason + "</white>"));
                logger.info("Kicked blacklisted player {} on reload: reason={}", player.getUsername(), entry.reason);
            }
        }
    }

    public synchronized BlacklistEntry getBlacklistEntry(Player player) {
        String username = player.getUsername().toLowerCase();
        String ip = player.getRemoteAddress().getAddress().getHostAddress();

        for (BlacklistEntry entry : entries) {
            if (entry.isExpired()) continue;
            if (entry.target.equals(username) || entry.target.equals(ip)) {
                return entry;
            }
        }
        return null;
    }

    public void startWatcher() {
        try {
            watchService = FileSystems.getDefault().newWatchService();
            blacklistFile.getParent().register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);

            watchThread = new Thread(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        WatchKey key = watchService.take();
                        for (WatchEvent<?> event : key.pollEvents()) {
                            Path changed = (Path) event.context();
                            if (changed.getFileName().toString().equals("blacklist.txt")) {
                                Thread.sleep(100);
                                reload();
                            }
                        }
                        key.reset();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (IOException e) {
                        logger.warn("Error reloading blacklist: {}", e.getMessage());
                    } catch (ClosedWatchServiceException e) {
                        break;
                    }
                }
            }, "autoserver-blacklist-watcher");
            watchThread.setDaemon(true);
            watchThread.start();
            logger.info("Blacklist file watcher started.");
        } catch (IOException e) {
            logger.warn("Failed to start blacklist watcher: {}", e.getMessage());
        }
    }

    public void stopWatcher() {
        if (watchThread != null) watchThread.interrupt();
        if (watchService != null) {
            try { watchService.close(); } catch (IOException ignored) {}
        }
    }
}
