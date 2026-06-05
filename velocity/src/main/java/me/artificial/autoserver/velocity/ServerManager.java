package me.artificial.autoserver.velocity;

import com.velocitypowered.api.proxy.Player;
import com.velocitypowered.api.proxy.server.RegisteredServer;
import com.velocitypowered.api.scheduler.ScheduledTask;
import com.velocitypowered.api.scheduler.Scheduler;
import me.artificial.autoserver.velocity.startable.LocalStartable;
import me.artificial.autoserver.velocity.startable.RemoteStartable;
import me.artificial.autoserver.velocity.startable.Startable;

import java.io.IOException;
import java.net.Socket;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * A ServerManager class that manages the state of servers, including starting, stopping,
 * and checking the status of servers.
 */
public class ServerManager {
    private final AutoServerLogger logger;
    private final AutoServer plugin;
    private final HashSet<String> startingServers = new HashSet<>();
    private final HashMap<Player, String> queuePlayers = new HashMap<>();
    private final Map<String, ServerStatus> serverStatusCache = new ConcurrentHashMap<>();
    private final Map<String, ScheduledTask> shutdownScheduledTask = new ConcurrentHashMap<>();

    public ServerManager(AutoServer plugin) {
        this.plugin = plugin;
        this.logger = plugin.getLogger();
    }

    /**
     * Starts a given server if it is not already running.
     *
     * @param server The server to start.
     * @return A CompletableFuture that completes with a success message if the server starts successfully,
     *         or completes exceptionally if an error occurs or the server is already running.
     */
    public CompletableFuture<String> startServer(RegisteredServer server) {
        // Check if already starting
        String serverName = server.getServerInfo().getName();
        if (startingServers.contains(serverName)) {
            logger.debug("Server {} is already starting", serverName);
            return CompletableFuture.completedFuture("Server is already starting.");
        }

        startingServers.add(serverName);

        logger.debug("Attempting to start server: {}", serverName);

        // Determine start strategy
        Startable startableStrategy = getServerStrategy(server);

        // Do a long ping to check if server is actually offline before trying to start it
        return isServerOnline(server)
                .thenCompose(isOnline -> {
                    if (isOnline) {
                        // Already running
                        moveQueuedPlayersToServer(server);
                        return CompletableFuture.completedFuture("Server already running");
                    }

                    // Finally start the server using the given strategy
                    return startableStrategy.start()
                            .thenCompose(result -> waitForServerToBecomeResponsive(server)
                                    .thenApply(isResponsive -> {
                                        if (isResponsive) {
                                            // Return the result after server becomes responsive.
                                            moveQueuedPlayersToServer(server);
                                            return "Server started and is responsive.";
                                        } else {
                                            // Return an error message if the server is not responsive.
                                            throw new RuntimeException("Server started but is not responsive.");
                                        }
                                    }));
                })
                .whenComplete((result, ex) -> {
                    // clean up
                    startingServers.remove(serverName);
                    if (ex != null) {
                        logger.error("Failed to start server: {}", ex.getMessage());
                    }
                });
    }

    /**
     * Stops a given server if it is currently running.
     *
     * @param server The server to stop.
     * @return A CompletableFuture that completes with a success message if the server stops successfully,
     *         or completes exceptionally if an error occurs or the server is already stopped.
     */
    public CompletableFuture<String> stopServer(RegisteredServer server) {
        // Check if already stopping
        String serverName = server.getServerInfo().getName();
        if (getServerStatus(server).isStopping()) {
            logger.debug("Server {} is already stopping", serverName);
            return CompletableFuture.completedFuture("Server is already stopping.");
        }

        getServerStatus(server).setStatus(ServerStatus.Status.STOPPING);

        logger.info("Attempting to stop server: {}", serverName);
        Startable startableStrategy = getServerStrategy(server);

        // Do a long ping to check if server is actually online before trying to stop it
        return isServerOnline(server)
                .thenCompose(isOnline -> {
                    if (!isOnline) {
                        return CompletableFuture.completedFuture("Server already stopped");
                    }

                    // Abort if players are still connected
                    if (!server.getPlayersConnected().isEmpty()) {
                        logger.info("Aborting stop of {}, {} player(s) still connected", serverName, server.getPlayersConnected().size());
                        getServerStatus(server).setStatus(ServerStatus.Status.UNKNOWN);
                        return CompletableFuture.completedFuture("Stop aborted, players online.");
                    }

                    // Finally stop the server using the given strategy
                    return startableStrategy.stop()
                            .thenCompose(result -> {
                                long shutdownDelay = plugin.getConfig().getShutdownDelay(server);

                                // Delay a little bit before trying to ping to give server time to stop
                                try {
                                    logger.info("Sleeping for {} seconds before checking if server has stopped.", shutdownDelay);
                                    Thread.sleep(shutdownDelay * 1000);
                                } catch (InterruptedException e) {
                                    logger.warn("Stop delay sleep interrupted: {}", e.getMessage());
                                    Thread.currentThread().interrupt();
                                }


                                return isServerOnline(server).thenApply(isOnline2 -> {
                                        if (isOnline2) {
                                            throw new RuntimeException("Failed to stop server.");
                                        } else {
                                            return "Server stopped.";
                                        }
                                    });
                            });
                })
                .whenComplete((result, ex) -> {
                    // clean up
                    if (ex != null) {
                        logger.error("Failed to stop server: {}", ex.getMessage());
                        getServerStatus(server).setStatus(ServerStatus.Status.UNKNOWN);
                    } else {
                        getServerStatus(server).setStatus(ServerStatus.Status.STOPPED);
                    }
                });
    }

    /**
     * Checks if the specified server is online (i.e., fully operational and ready to accept connections).
     *
     * @param server The server to check.
     * @return A CompletableFuture that completes with true if the server is online, false otherwise.
     */
    public CompletableFuture<Boolean> isServerOnline(RegisteredServer server) {
        return pingServer(server, 5000);
    }

    /**
     * Performs a quick check on server responsiveness, including checking cache and optionally pinging the server.
     * The state of the server may not be guaranteed with this check.
     *
     * @param server The server to check.
     * @return A CompletableFuture that completes with true if the server is responsive, false otherwise.
     */
    public CompletableFuture<Boolean> isServerResponsive(RegisteredServer server) {
        String serverName = server.getServerInfo().getName();
        ServerStatus cachedStatus = getServerStatus(server);

        if (cachedStatus.is(ServerStatus.Status.STOPPED)) {
            logger.debug("Cache check for server '{}' is OFFLINE", serverName);
            return CompletableFuture.completedFuture(false);
        }
        if (!server.getPlayersConnected().isEmpty()) {
            logger.debug("Players detected on server '{}', assuming ONLINE", serverName);
            return CompletableFuture.completedFuture(true);
        }

        // cache not valid need to ping
        return pingServer(server, 50);
    }

    /**
     * Retrieves the current status of the specified server.
     *
     * @param server The server whose status is to be retrieved.
     * @return The current status of the server (e.g., ONLINE, OFFLINE, UNKNOWN).
     */
    public ServerStatus getServerStatus(RegisteredServer server) {
        String serverName = server.getServerInfo().getName();
        if (!serverStatusCache.containsKey(serverName)) {
            serverStatusCache.put(serverName, new ServerStatus());
            try {
                isServerOnline(server).get();
            } catch (InterruptedException | ExecutionException ignored) {
            }
        }

        return serverStatusCache.get(serverName);
    }

    /**
     * Queues a player to join a server once it's started and available.
     *
     * @param player The player to queue.
     * @param serverName The name of the server the player is waiting to join.
     */
    public void queuePlayerForServerJoin(Player player, String serverName) {
        queuePlayers.put(player, serverName);
    }

    /**
     * Schedules server for shutdown.
     *
     * @param server The server to be scheduled for shutdown.
     */
    public void scheduleShutdownServer(RegisteredServer server) {
        assert server != null;
        logger.trace("scheduleShutdownServer: {}", server.getServerInfo().getName());
        long autoShutdownDelay = plugin.getConfig().getAutoShutdownDelay(server);
        if (autoShutdownDelay <= 0) {
            return;
        }

        assert !shutdownScheduledTask.containsKey(server.getServerInfo().getName()) : "Server already has task scheduled for shutdown.";
        logger.info("Scheduling shutdown of server {} in {}", server.getServerInfo().getName(), autoShutdownDelay);

        Scheduler.TaskBuilder taskBuilder = plugin.getProxy().getScheduler()
                .buildTask(plugin,() -> {
                    assert server.getPlayersConnected().isEmpty() : "Server is not empty";
                    stopServer(server).whenComplete((result, ex) -> {
                        if (ex != null) {
                            logger.error("error: {}", ex.getMessage());
                        } else {
                            logger.info("Message: {}", result);
                        }
                    });
                }).delay(Duration.ofSeconds(autoShutdownDelay));

        ScheduledTask scheduledTask = taskBuilder.schedule();
        shutdownScheduledTask.put(server.getServerInfo().getName(), scheduledTask);
    }

    /**
     * Continuously polls a target server until it is online and not full,
     * starting it if necessary, then transfers the player from limbo.
     *
     * @param player       The player waiting in limbo.
     * @param targetServer The server to transfer the player to when ready.
     */
    public void pollAndTransferWhenReady(Player player, RegisteredServer targetServer) {
        String targetName = targetServer.getServerInfo().getName();
        logger.info("[limbo-poll] Starting poll loop for player {} targeting {}", player.getUsername(), targetName);

        plugin.getProxy().getScheduler().buildTask(plugin, () -> {
            // Check player still online
            if (!player.isActive()) {
                logger.info("[limbo-poll] Player {} is no longer active, stopping poll", player.getUsername());
                return;
            }

            // Check player still in limbo
            boolean inLimbo = player.getCurrentServer()
                    .map(sc -> sc.getServerInfo().getName().equals("limbo"))
                    .orElse(false);
            if (!inLimbo) {
                logger.info("[limbo-poll] Player {} is no longer in limbo, stopping poll", player.getUsername());
                return;
            }

            logger.debug("[limbo-poll] Polling {} for player {}", targetName, player.getUsername());

            isServerResponsive(targetServer).thenAccept(online -> {
                if (!online) {
                    logger.info("[limbo-poll] {} is offline, starting it for player {}", targetName, player.getUsername());
                    Messenger.send(player, plugin.getConfig().getMessage("starting").orElse(""), targetName);
                    cancelShutdownServer(targetServer);
                    // Only start if not already starting
                    if (!startingServers.contains(targetName)) {
                        startServer(targetServer).exceptionally(ex -> {
                            logger.error("[limbo-poll] Failed to start {}: {}", targetName, ex.getMessage());
                            return null;
                        });
                    }
                    // Reschedule poll — do not double-handle here, startServer will move queued players
                    scheduleNextPoll(player, targetServer);
                    return;
                }

                // Server is online, try to connect
                cancelShutdownServer(targetServer);
                Messenger.send(player, plugin.getConfig().getMessage("notify").orElse(""), targetName);
                plugin.internalTransfer(player);
                player.createConnectionRequest(targetServer).connect().whenComplete((result, ex) -> {
                    if (ex != null || !result.isSuccessful()) {
                        logger.warn("[limbo-poll] Transfer of {} to {} failed, will retry: {}",
                                player.getUsername(), targetName, ex != null ? ex.getMessage() : result.getReasonComponent());
                        Messenger.send(player, plugin.getConfig().getMessage("failed").orElse(""), targetName);
                        // reschedule
                        scheduleNextPoll(player, targetServer);
                    } else {
                        logger.info("[limbo-poll] Player {} successfully transferred to {}", player.getUsername(), targetName);
                    }
                });
            });
        }).delay(java.time.Duration.ofSeconds(10)).schedule();
    }

    private void scheduleNextPoll(Player player, RegisteredServer targetServer) {
        if (!player.isActive()) return;
        boolean inLimbo = player.getCurrentServer()
                .map(sc -> sc.getServerInfo().getName().equals("limbo"))
                .orElse(false);
        if (!inLimbo) return;
        pollAndTransferWhenReady(player, targetServer);
    }

    public void cancelShutdownServer(RegisteredServer server) {
        String serverName = server.getServerInfo().getName();
        if (shutdownScheduledTask.containsKey(serverName)) {
            logger.info("Cancelling auto shutdown: {}", serverName);
            shutdownScheduledTask.get(serverName).cancel();
            shutdownScheduledTask.remove(serverName);
        }
    }

    public void validateServers(Collection<RegisteredServer> servers) {
        logger.trace("Validating Server status...");
        for (RegisteredServer server : servers) {
            pingServer(server, 5000).thenApply((isOnline) -> {
                if (isOnline && server.getPlayersConnected().isEmpty()) {
                    String serverName = server.getServerInfo().getName();
                    if (shutdownScheduledTask.containsKey(serverName)) {
                        logger.trace("Server {} is already scheduled to stop", serverName);
                        return null;
                    }

                    scheduleShutdownServer(server);
                }
                return null;
            });
        }
    }

    private Startable getServerStrategy(RegisteredServer server) {
        Optional<Boolean> remote = plugin.getConfig().isRemoteServer(server);
        if (remote.isPresent() && remote.get()) {
            return new RemoteStartable(plugin, server);
        }
        return new LocalStartable(plugin, server);
    }

    private CompletableFuture<Boolean> pingServer(RegisteredServer server, int pingTimeout) {
        String serverName = server.getServerInfo().getName();
        logger.debug("Pinging server {}...", serverName);
        return server.ping().orTimeout(pingTimeout, TimeUnit.MILLISECONDS).thenApply(serverPing -> {
            logger.debug("ping success {} is {}online{}", serverName, AnsiColors.GREEN, AnsiColors.RESET);
            if (!getServerStatus(server).isStopping()) {
                // only update to running if not in a state of stopping
                getServerStatus(server).setStatus(ServerStatus.Status.RUNNING);
            }
            return true;
        }).exceptionallyCompose(e -> {
            logger.debug("ping failed for {}: {}", serverName, e.getMessage());
            // Handle large or malformed packet errors
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            String msg = cause.getMessage() != null ? cause.getMessage() : "";
            if (msg.contains("A packet did not decode successfully")) {
                logger.debug("failed to decode packet, likely online trying socket connect");
                // good chance the server is online check with a socket connect
                return CompletableFuture.supplyAsync(() -> {
                    try (Socket socket = new Socket()) {
                        socket.connect(server.getServerInfo().getAddress());
                        logger.warn("Socket connection to {} succeeded, treating as online.", serverName);
                        if (!getServerStatus(server).isStopping()) {
                            getServerStatus(server).setStatus(ServerStatus.Status.RUNNING);
                        }
                        return true;
                    } catch (IOException ioe) {
                        logger.warn("Socket connection to {} failed after ping error.", serverName);
                        if (!getServerStatus(server).isStarting()) {
                            getServerStatus(server).setStatus(ServerStatus.Status.STOPPED);
                        }
                        return false;
                    }
                });
            }

            logger.debug("ping failed {} is {}offline{}", serverName, AnsiColors.RED, AnsiColors.RESET);
            if (!getServerStatus(server).isStarting()) {
                // only update to stopped if not in a state of starting
                getServerStatus(server).setStatus(ServerStatus.Status.STOPPED);
            }
            return CompletableFuture.completedFuture(false);
        });
    }

    /**
     * Continuously pings the specified server until it becomes responsive and ready.
     *
     * @param server The server to ping.
     * @return A CompletableFuture that completes with true when the server is ready, false if an error occurs or the server does not respond.
     */
    private CompletableFuture<Boolean> waitForServerToBecomeResponsive(RegisteredServer server) {
        return CompletableFuture.supplyAsync(() ->{
            int retires = 10;
            int delayBetweenRetries = 5; // seconds
            long startupDelay = plugin.getConfig().getStartUpDelay(server);

            // Delay a little bit before trying to ping to give server time to start
            try {
                logger.info("Sleeping for {} seconds before checking if server has started.", startupDelay);
                Thread.sleep(startupDelay * 1000);
            } catch (InterruptedException e) {
                logger.warn("Ping delay sleep interrupted: {}", e.getMessage());
                Thread.currentThread().interrupt();
            }

            // ping server with retires
            while (retires > 0) {
                try {
                    if (pingServer(server, 5000).get()) {
                        logger.info("Server {} is {}online{}. Moving queued players...", server.getServerInfo().getName(), AnsiColors.GREEN, AnsiColors.RESET);
                        return true;
                    } else {
                        logger.debug("Failed to ping server {}. Retrying in {} seconds.", server.getServerInfo().getName(), delayBetweenRetries);
                    }
                } catch (ExecutionException | InterruptedException e) {
                    logger.debug("Failed to ping server {}: {}. Retrying in {} seconds.", server.getServerInfo().getName(), e.getMessage(), delayBetweenRetries);
                }

                retires--;
                if (retires > 0) {
                    try {
                        Thread.sleep(delayBetweenRetries * 1000L);
                    } catch (InterruptedException e) {
                        logger.warn("Ping retry sleep interrupted: {}", e.getMessage());
                        Thread.currentThread().interrupt();
                    }
                }
            }

            return false;
        });
    }

    /**
     * Moves all players from the queue to the specified server once the server has started and is ready.
     *
     * @param server The server to which queued players will be moved.
     */
    private void moveQueuedPlayersToServer(RegisteredServer server) {
        queuePlayers.forEach((player, serverName) -> {
            if (serverName.equals(server.getServerInfo().getName())) {
                if (player.isActive()) {
                    // Notify the player
                    if (player.getCurrentServer().isPresent()) {
                        Messenger.send(player, plugin.getConfig().getMessage("notify").orElse(""), serverName);
                        // Schedule the connection request to run after 5 seconds
                        plugin.getProxy().getScheduler().buildTask(plugin, () -> {
                            plugin.internalTransfer(player);
                            player.createConnectionRequest(server).connect().whenComplete((result, throwable) -> {
                                if (throwable != null) {
                                    Messenger.send(player, plugin.getConfig().getMessage("failed").orElse(""), serverName);
                                    logger.error("Failed to connect player to server {}", throwable.getMessage());
                                } else {
                                    logger.info("Player {} successfully moved to server {}", player.getUsername(), serverName);
                                }
                                queuePlayers.remove(player);
                            });
                        }).delay(5, TimeUnit.SECONDS).schedule();
                    } else {
                        // Not connected to a server so want to connect fast
                        player.createConnectionRequest(server).connect().whenComplete((result, throwable) -> {
                            if (throwable != null) {
                                Messenger.send(player, plugin.getConfig().getMessage("failed").orElse(""), serverName);
                                logger.error("Failed to connect player to server {}", throwable.getMessage());
                            } else {
                                logger.info("Player {} successfully moved to server {}", player.getUsername(), serverName);
                            }
                            queuePlayers.remove(player);
                        });
                    }

                }
            }
        });
    }
}
