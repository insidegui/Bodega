import Foundation

/// Represents an event that causes items in a storage engine to be invalidated.
public enum StoreEvent: Hashable {
    /// Sent when items are added or updated.
    case update([CacheKey])
    /// Sent when specific items are removed.
    case remove([CacheKey])
    /// Sent when all items are removed.
    case removeAll
}

/// Protocol adopted by storage engines that can publish notifications when changes are made to their data.
public protocol NotifyingStorageEngine: StorageEngine {
    var incomingEvents: AsyncStream<StoreEvent> { get }
}
