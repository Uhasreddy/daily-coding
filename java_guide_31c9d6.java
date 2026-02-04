// Learning Objective:
// This tutorial demonstrates how to build a basic data pipeline using Java's `Flow` API,
// focusing on the core concepts of Publisher, Subscriber, and Subscription,
// and implementing a backpressure mechanism. You will learn how a Subscriber
// controls the flow of data from a Publisher by explicitly requesting items.

import java.util.concurrent.Flow; // Import the Flow API interfaces
import java.util.concurrent.atomic.AtomicLong; // For thread-safe counter for requested items
import java.util.concurrent.ExecutorService; // To run operations asynchronously
import java.util.concurrent.Executors; // To create an ExecutorService
import java.util.concurrent.TimeUnit; // For Thread.sleep (used in main for demo)

/**
 * A custom implementation of Flow.Publisher that generates a sequence of integers.
 * This Publisher demonstrates how to manage subscriptions and send items only
 * when requested by the Subscriber, adhering to backpressure.
 *
 * Concepts taught: Publisher, Subscription creation, onSubscribe call.
 */
class SimpleIntPublisher implements Flow.Publisher<Integer> {

    private final String name;
    private final ExecutorService executor; // Using an executor to simulate asynchronous work

    public SimpleIntPublisher(String name, ExecutorService executor) {
        this.name = name;
        this.executor = executor;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super Integer> subscriber) {
        // When a subscriber connects, we create a Subscription specific to that subscriber.
        SimpleIntSubscription subscription = new SimpleIntSubscription(subscriber);
        // The first thing a Publisher must do is call the Subscriber's onSubscribe method.
        // This establishes the link, giving the Subscriber a handle to control the flow.
        subscriber.onSubscribe(subscription);
        System.out.println(name + ": Subscriber " + subscriber.getClass().getSimpleName() + " subscribed.");
    }

    /**
     * Represents the bridge between the Publisher and a specific Subscriber.
     * It manages the demand (how many items the Subscriber has requested) and
     * is responsible for sending items to the Subscriber.
     *
     * Concepts taught: Subscription, request(long n), cancel(), backpressure logic.
     */
    private class SimpleIntSubscription implements Flow.Subscription {
        private final Flow.Subscriber<? super Integer> subscriber;
        // AtomicLong for thread-safe tracking of requested items.
        // It's crucial for backpressure: Publisher only sends if this count is > 0.
        private final AtomicLong requested = new AtomicLong(0);
        private volatile boolean isCanceled = false; // Flag to stop production if canceled
        private int currentItem = 0; // The next item to be sent

        public SimpleIntSubscription(Flow.Subscriber<? super Integer> subscriber) {
            this.subscriber = subscriber;
        }

        @Override
        public void request(long n) {
            if (n <= 0) {
                // As per Reactive Streams specification, requesting a non-positive number
                // is an error condition. We should signal onError and cancel.
                subscriber.onError(new IllegalArgumentException("Requested items must be positive"));
                cancel();
                return;
            }

            // Atomically add the new request to the total.
            // This ensures thread safety if multiple threads request concurrently.
            long totalRequested = requested.getAndAdd(n);
            System.out.println(name + ": Subscriber requested " + n + " items (total demand: " + (totalRequested + n) + ")");

            // Schedule item production in the executor.
            // This decouples item generation from the request call,
            // simulating asynchronous processing and preventing blocking the caller.
            executor.submit(this::produceItems);
        }

        private void produceItems() {
            // Keep producing as long as there are items requested and the subscription is not canceled.
            while (requested.get() > 0 && !isCanceled) {
                try {
                    // Simulate work or delay in producing an item.
                    Thread.sleep(50); // Small delay to visualize flow

                    // Try to "claim" one item from the requested demand.
                    // This is crucial for backpressure: decrement the requested count *before* sending.
                    if (requested.decrementAndGet() >= 0) {
                        int itemToSend = currentItem++;
                        System.out.println(name + ": Sending item " + itemToSend);
                        subscriber.onNext(itemToSend); // Send the item to the subscriber

                        // In a real scenario, you might have a finite source.
                        // For this example, we'll stop after 20 items for demonstration.
                        if (itemToSend >= 20) {
                             subscriber.onComplete(); // Signal that the stream is finished
                             cancel(); // Stop further production
                             break;
                        }
                    } else {
                        // If decrementAndGet made it negative, it means a race condition
                        // or cancellation happened just before we tried to decrement.
                        // Increment it back and break the loop, as we shouldn't send.
                        requested.incrementAndGet();
                        break;
                    }

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // Restore interrupt status
                    subscriber.onError(e); // Inform subscriber about the error
                    cancel();
                    break;
                } catch (Exception e) {
                    subscriber.onError(e);
                    cancel();
                    break;
                }
            }
            if (isCanceled) {
                System.out.println(name + ": Production stopped due to cancellation.");
            }
            // If requested.get() == 0, we simply stop producing until more items are requested.
            // The executor will take care of resubmitting produceItems if more requests come in.
        }


        @Override
        public void cancel() {
            isCanceled = true; // Set flag to stop production
            // For this simple example, setting the flag is enough.
            // In complex scenarios, you might need to interrupt running tasks.
            System.out.println(name + ": Subscription cancelled.");
        }
    }
}

/**
 * A custom implementation of Flow.Subscriber that consumes integers.
 * This Subscriber demonstrates how to handle items with backpressure,
 * requesting items in batches to avoid being overwhelmed.
 *
 * Concepts taught: Subscriber, onSubscribe, onNext, onError, onComplete,
 *                  requesting more items (backpressure).
 */
class SimpleIntSubscriber implements Flow.Subscriber<Integer> {

    private final String name;
    private Flow.Subscription subscription; // The link to the Publisher, used for backpressure
    private final int batchSize; // How many items to request at a time
    private int receivedCount = 0; // Counter for items received

    public SimpleIntSubscriber(String name, int batchSize) {
        this.name = name;
        this.batchSize = batchSize;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        // This is the first method called by the Publisher.
        // We store the Subscription object to control the flow (request/cancel).
        this.subscription = subscription;
        System.out.println(name + ": Subscribed. Requesting initial " + batchSize + " items.");
        // Initially request a batch of items. This kicks off the data flow.
        subscription.request(batchSize);
    }

    @Override
    public void onNext(Integer item) {
        // This method is called by the Publisher for each item.
        receivedCount++;
        System.out.println(name + ": Received item " + item + " (Total: " + receivedCount + ")");

        // Simulate some processing time for the item.
        // This subscriber processes slower than the publisher produces,
        // which highlights the need for backpressure.
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            onError(e); // Propagate error
            return;
        }

        // After processing an item, we check if we need more.
        // This is the core of backpressure: we only request more when ready
        // and after processing a full batch.
        if (receivedCount % batchSize == 0) {
            System.out.println(name + ": Processed " + batchSize + " items. Requesting " + batchSize + " more.");
            subscription.request(batchSize); // Request the next batch
        }
    }

    @Override
    public void onError(Throwable throwable) {
        // Called if an error occurs in the Publisher or Subscription.
        System.err.println(name + ": ERROR: " + throwable.getMessage());
        // In a real application, you might log the error, release resources, etc.
    }

    @Override
    public void onComplete() {
        // Called when the Publisher has no more items to send.
        System.out.println(name + ": COMPLETED. Total items received: " + receivedCount);
        // Release any resources if necessary.
    }
}

/**
 * Main class to demonstrate the data pipeline with backpressure using the Flow API.
 * This sets up the Publisher and Subscriber and connects them.
 */
public class FlowBackpressureDemo {

    public static void main(String[] args) throws InterruptedException {
        // Create an ExecutorService to run the Publisher's and Subscriber's
        // asynchronous tasks. This is good practice for non-blocking operations.
        // Using separate executors to simulate distinct components doing their work.
        ExecutorService publisherExecutor = Executors.newSingleThreadExecutor();
        // The Subscriber's onNext method runs on the thread that calls it (usually the Publisher's).
        // However, if the Subscriber itself needs to do long-running asynchronous work
        // *after* receiving an item, it would use its own executor for that internal work.
        // For simplicity, we'll just demonstrate the Publisher's use of an executor.

        // 1. Create a Publisher: This will produce integers.
        SimpleIntPublisher publisher = new SimpleIntPublisher("MyPublisher", publisherExecutor);

        // 2. Create a Subscriber: This will consume integers, requesting them in batches.
        // The `batchSize` determines how many items the subscriber requests at a time.
        // A smaller batchSize implies stronger backpressure control.
        SimpleIntSubscriber subscriber = new SimpleIntSubscriber("MySubscriber", 5);

        // 3. Connect the Publisher and Subscriber:
        // The Publisher's subscribe method will initiate the handshake,
        // calling `subscriber.onSubscribe()` which in turn makes the first `request()`.
        publisher.subscribe(subscriber);

        // Allow some time for the flow to complete.
        // In a real application, you'd have more sophisticated lifecycle management
        // (e.g., waiting for the onComplete signal or a timeout).
        System.out.println("\n--- Starting data flow ---\n");
        Thread.sleep(3000); // Wait for 3 seconds to observe the interaction

        // Shut down the executor gracefully.
        // This is important to release resources and prevent the application from hanging.
        publisherExecutor.shutdown();
        publisherExecutor.awaitTermination(1, TimeUnit.SECONDS); // Wait for tasks to finish
        System.out.println("\n--- Application finished ---");
    }
}