import Foundation
import Combine

public protocol NotifyingStorageEngine: StorageEngine {

    var didCreateItems: PassthroughSubject<[CacheKey], Never> { get }
    var didUpdateItems: PassthroughSubject<[CacheKey], Never> { get }
    var didRemoveItems: PassthroughSubject<[CacheKey], Never> { get }

    func observeNotifications()

}
