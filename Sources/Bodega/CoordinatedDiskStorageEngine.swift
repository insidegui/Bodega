import Foundation
import Combine

/// A ``StorageEngine`` based on saving items to the file system in a coordinated fashion, allowing multiple
/// processes to safely access the same underlying file storage without corruption.
///
/// This engine works in a very similar way to ``DiskStorageEngine``, with the difference that
/// it uses `NSFileCoordinator` in order to ensure safe access to the files on disk,
/// allowing multiple processes to use the same underlying storage without data corruption.
///
/// Using `NSFileCoordinator` also allows processes using the same on-disk storage to
/// receive notifications and update their in-memory cache with changes made by other processes.
public actor CoordinatedDiskStorageEngine: NotifyingStorageEngine {

    /// A directory on the filesystem where your ``StorageEngine``s data will be stored.
    private let directory: FileManager.Directory

    private let filePresenter: FilePresenter
    private let fileCoordinator: NSFileCoordinator

    /// Initializes a new ``DiskStorageEngine`` for persisting `Data` to disk.
    /// - Parameter directory: A directory on the filesystem where your files will be written to.
    /// `FileManager.Directory` is a type-safe wrapper around URL that provides sensible defaults like
    ///  `.documents(appendingPath:)`, `.caches(appendingPath:)`, and more.
    public init(directory: FileManager.Directory) {
        self.directory = directory

        let presenter = FilePresenter(url: directory.url)
        self.fileCoordinator = NSFileCoordinator(filePresenter: presenter)
        self.filePresenter = presenter
    }

    /// Writes `Data` to disk based on the associated ``CacheKey``.
    /// - Parameters:
    ///   - data: The `Data` being stored to disk.
    ///   - key: A ``CacheKey`` for matching `Data` to a location on disk.
    public func write(_ data: Data, key: CacheKey) async throws {
        let fileURL = self.concatenatedPath(key: key.value)
        let folderURL = fileURL.deletingLastPathComponent()

        try await withCheckedThrowingContinuation { continuation in
            self.fileCoordinator.coordinate(with: [.writingIntent(with: folderURL), .writingIntent(with: fileURL)], queue: .main) { error in
                do {
                    if let error { throw error }

                    if !Self.directoryExists(atURL: folderURL) {
                        try Self.createDirectory(url: folderURL)
                    }

                    try data.write(to: fileURL, options: .atomic)

                    continuation.resume()
                } catch {
                    continuation.resume(throwing: error)
                }
            }
        }
    }

    /// Writes an array of `Data` items to disk based on the associated ``CacheKey`` passed in the tuple.
    /// - Parameters:
    ///   - dataAndKeys: An array of the `[(CacheKey, Data)]` to store
    ///   multiple `Data` items with their associated keys at once.
    public func write(_ dataAndKeys: [(key: CacheKey, data: Data)]) async throws {
        for dataAndKey in dataAndKeys {
            try await self.write(dataAndKey.data, key: dataAndKey.key)
        }
    }

    /// Reads `Data` from disk based on the associated ``CacheKey``.
    /// - Parameters:
    ///   - key: A ``CacheKey`` for matching `Data` to a location on disk.
    /// - Returns: The `Data` stored on disk if it exists, nil if there is no `Data` stored for the `CacheKey`.
    public func read(key: CacheKey) async -> Data? {
        let fileURL = self.concatenatedPath(key: key.value)

        return try? await withCheckedThrowingContinuation { continuation in
            self.fileCoordinator.coordinate(with: [.readingIntent(with: fileURL)], queue: .main) { error in
                do {
                    if let error { throw error }

                    let data = try Data(contentsOf: fileURL)

                    continuation.resume(returning: data)
                } catch {
                    continuation.resume(throwing: error)
                }
            }
        }
    }

    /// Reads `Data` from disk based on the associated array of ``CacheKey``s provided as a parameter
    /// and returns an array `[(CacheKey, Data)]` associated with the passed in ``CacheKey``s.
    ///
    /// This method returns the ``CacheKey`` and `Data` together in a tuple of `[(CacheKey, Data)]`
    /// allowing you to know which ``CacheKey`` led to a specific `Data` item being retrieved.
    /// This can be useful in allowing manual iteration over data, but if you don't need
    /// to know which ``CacheKey`` that led to a piece of `Data` being retrieved
    ///  you can use ``read(keys:)`` instead.
    /// - Parameters:
    ///   - keys: A `[CacheKey]` for matching multiple `Data` items.
    /// - Returns: An array of `[(CacheKey, Data)]` read from disk if the ``CacheKey``s exist,
    /// and an empty array if there are no `Data` items matching the `keys` passed in.
    public func readDataAndKeys(keys: [CacheKey]) async -> [(key: CacheKey, data: Data)] {
        return zip(
            keys,
            await self.read(keys: keys)
        ).map { ($0, $1) }
    }

    /// Reads all the `[Data]` located in the `directory`.
    /// - Returns: An array of the `[Data]` contained on disk.
    public func readAllData() async -> [Data] {
        let allKeys = await self.allKeys()
        return await self.read(keys: allKeys)
    }

    /// Reads all the `Data` located in the `directory` and returns an array
    /// of `[(CacheKey, Data)]` tuples associated with the ``CacheKey``.
    ///
    /// This method returns the ``CacheKey`` and `Data` together in an array of `[(CacheKey, Data)]`
    /// allowing you to know which ``CacheKey`` led to a specific `Data` item being retrieved.
    /// This can be useful in allowing manual iteration over `Data` items, but if you don't need
    /// to know which ``CacheKey`` led to a piece of `Data` being retrieved
    /// you can use ``readAllData()`` instead.
    /// - Returns: An array of the `[Data]` and it's associated `CacheKey`s contained in a directory.
    public func readAllDataAndKeys() async -> [(key: CacheKey, data: Data)] {
        let allKeys = await self.allKeys()
        return await self.readDataAndKeys(keys: allKeys)
    }

    /// Removes `Data` from disk based on the associated ``CacheKey``.
    /// - Parameters:
    ///   - key: A ``CacheKey`` for matching `Data` to a location on disk.
    public func remove(key: CacheKey) async throws {
        let fileURL = self.concatenatedPath(key: key.value)

        try await withCheckedThrowingContinuation { continuation in
            self.fileCoordinator.coordinate(with: [.writingIntent(with: fileURL, options: .forDeleting)], queue: .main) { error in
                do {
                    if let error { throw error }

                    try FileManager.default.removeItem(at: fileURL)

                    continuation.resume()
                } catch CocoaError.fileNoSuchFile {
                    // No-op, we treat deleting a non-existent file/folder as a successful removal rather than throwing
                    continuation.resume()
                } catch {
                    continuation.resume(throwing: error)
                }
            }
        }
    }

    /// Removes all the `Data` items located in the `directory`.
    public func removeAllData() async throws {
        let dirURL = self.directory.url

        try await withCheckedThrowingContinuation { continuation in
            self.fileCoordinator.coordinate(with: [.writingIntent(with: dirURL, options: .forDeleting)], queue: .main) { error in
                do {
                    if let error { throw error }

                    try FileManager.default.removeItem(at: dirURL)

                    continuation.resume()
                } catch CocoaError.fileNoSuchFile {
                    // No-op, we treat deleting a non-existent file/folder as a successful removal rather than throwing
                    continuation.resume()
                } catch {
                    continuation.resume(throwing: error)
                }
            }
        }
    }

    /// Checks whether a value with a key is persisted.
    /// - Parameter key: The key to for existence.
    /// - Returns: If the key exists the function returns true, false if it does not.
    public func keyExists(_ key: CacheKey) async -> Bool {
        await self.allKeys().contains(key)
    }

    /// Iterates through a directory to find the total number of `Data` items.
    /// - Returns: The file/key count.
    public func keyCount() async -> Int {
        return await self.allKeys().count
    }

    /// Iterates through a `directory` to find all of the keys.
    /// - Returns: An array of the keys contained in a directory.
    public func allKeys() async -> [CacheKey] {
        let dirURL = self.directory.url

        return await withCheckedContinuation { continuation in
            self.fileCoordinator.coordinate(with: [.readingIntent(with: dirURL, options: .immediatelyAvailableMetadataOnly)], queue: .main) { error in
                do {
                    if let error { throw error }

                    let directoryContents = try FileManager.default.contentsOfDirectory(at: dirURL, includingPropertiesForKeys: nil)
                    let fileOnlyKeys = directoryContents.lazy.filter({ !$0.hasDirectoryPath }).map(\.lastPathComponent)

                    let keys = fileOnlyKeys.map(CacheKey.init(verbatim:))

                    continuation.resume(returning: Array(keys))
                } catch {
                    continuation.resume(returning: [])
                }
            }
        }
    }

    /// Returns the date of creation for the file represented by the ``CacheKey``, if it exists.
    /// - Parameters:
    ///   - key: A ``CacheKey`` for matching `Data` to a location on disk.
    /// - Returns: The creation date of the `Data` on disk if it exists, nil if there is no `Data` stored for the `CacheKey`.
    public func createdAt(key: CacheKey) async -> Date? {
        await readMetadata(resourceKey: .creationDateKey, keyPath: \.creationDate, from: key)
    }

    /// Returns the updatedAt date for the file represented by the ``CacheKey``, if it exists.
    /// - Parameters:
    ///   - key: A ``CacheKey`` for matching `Data` to a location on disk.
    /// - Returns: The updatedAt date of the `Data` on disk if it exists, nil if there is no `Data` stored for the ``CacheKey``.
    public func updatedAt(key: CacheKey) async -> Date? {
        await readMetadata(resourceKey: .contentModificationDateKey, keyPath: \.contentModificationDate, from: key)
    }

    /// Returns the last access date of the file for the ``CacheKey``, if it exists.
    /// - Parameters:
    ///   - key: A ``CacheKey`` for matching `Data` to a location on disk.
    /// - Returns: The last access date of the `Data` on disk if it exists, nil if there is no `Data` stored for the ``CacheKey``.
    public func lastAccessed(key: CacheKey) async -> Date? {
        await readMetadata(resourceKey: .contentAccessDateKey, keyPath: \.contentAccessDate, from: key)
    }

    public var incomingEvents: AsyncStream<StoreEvent> {
        AsyncStream { continuation in
            let cancellable = self.filePresenter.fileChangeSubject
                .map { change in
                    switch change {
                    case .modified(let url):
                        return .update([CacheKey(verbatim: url.lastPathComponent)])
                    case .deleted(let url):
                        return .remove([CacheKey(verbatim: url.lastPathComponent)])
                    }
                }
                .sink { event in
                    continuation.yield(event)
                }

            continuation.onTermination = { _ in
                cancellable.cancel()
            }
        }
    }

}

private extension CoordinatedDiskStorageEngine {

    static func createDirectory(url: URL) throws {
        try FileManager.default
            .createDirectory(
                at: url,
                withIntermediateDirectories: true,
                attributes: nil
            )
    }

    static func directoryExists(atURL url: URL) -> Bool {
        var isDirectory: ObjCBool = true

        return FileManager.default.fileExists(atPath: url.path, isDirectory: &isDirectory)
    }

    func concatenatedPath(key: String) -> URL {
        return self.directory.url.appendingPathComponent(key)
    }

    func readMetadata<V>(resourceKey: URLResourceKey, keyPath: KeyPath<URLResourceValues, V?>, from key: CacheKey) async -> V? {
        let fileURL = self.concatenatedPath(key: key.value)

        return await withCheckedContinuation { continuation in
            self.fileCoordinator.coordinate(with: [.readingIntent(with: fileURL, options: .immediatelyAvailableMetadataOnly)], queue: .main) { _ in
                let value = try? fileURL.resourceValues(forKeys: [resourceKey])[keyPath: keyPath]
                continuation.resume(returning: value)
            }
        }
    }

    final class FilePresenter: NSObject, NSFilePresenter {

        let presentedItemURL: URL?

        let presentedItemOperationQueue = OperationQueue.main

        enum FileChange: Hashable {
            case modified(URL)
            case deleted(URL)
        }

        let fileChangeSubject = PassthroughSubject<FileChange, Never>()

        init(url: URL) {
            self.presentedItemURL = url

            super.init()

            NSFileCoordinator.addFilePresenter(self)
        }

        func presentedItemDidChange() {
            print("👁️ presentedItemDidChange")
        }

        func accommodatePresentedItemDeletion() async throws {
            print("👁️ accommodatePresentedItemDeletion")
        }

        func presentedSubitemDidAppear(at url: URL) {
            print("👁️ presentedSubitemDidAppear \(url.lastPathComponent)")

            fileChangeSubject.send(.modified(url))
        }

        func presentedSubitemDidChange(at url: URL) {
            print("👁️ presentedSubitemDidChange \(url.lastPathComponent)")

            if FileManager.default.fileExists(atPath: url.path) {
                fileChangeSubject.send(.modified(url))
            } else {
                fileChangeSubject.send(.deleted(url))
            }
        }

        func accommodatePresentedSubitemDeletion(at url: URL) async throws {
            print("👁️ accommodatePresentedSubitemDeletion \(url.lastPathComponent)")

            fileChangeSubject.send(.deleted(url))
        }

        deinit {
            NSFileCoordinator.removeFilePresenter(self)
        }

    }

}
