<?php

/*
 * This file is part of the Symfony package.
 *
 * (c) Fabien Potencier <fabien@symfony.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Symfony\Component\Cache\Adapter;

use Predis\Connection\Aggregate\ClusterInterface;
use Predis\Connection\Aggregate\PredisCluster;
use Predis\Connection\Aggregate\ReplicationInterface;
use Predis\Response\ErrorInterface;
use Predis\Response\Status;
use Symfony\Component\Cache\CacheItem;
use Symfony\Component\Cache\Exception\InvalidArgumentException;
use Symfony\Component\Cache\Exception\LogicException;
use Symfony\Component\Cache\Marshaller\DeflateMarshaller;
use Symfony\Component\Cache\Marshaller\MarshallerInterface;
use Symfony\Component\Cache\Marshaller\TagAwareMarshaller;
use Symfony\Component\Cache\PruneableInterface;
use Symfony\Component\Cache\Traits\RedisClusterProxy;
use Symfony\Component\Cache\Traits\RedisProxy;
use Symfony\Component\Cache\Traits\RedisTrait;

/**
 * Stores tag id <> cache id relationship as a Redis Set.
 *
 * Set (tag relation info) is stored without expiry (non-volatile), while cache always gets an expiry (volatile) even
 * if not set by caller. Thus if you configure redis with the right eviction policy you can be safe this tag <> cache
 * relationship survives eviction (cache cleanup when Redis runs out of memory).
 *
 * Redis server 2.8+ with any `volatile-*` eviction policy, OR `noeviction` if you're sure memory will NEVER fill up
 *
 * Design limitations:
 *  - Max 4 billion cache keys per cache tag as limited by Redis Set datatype.
 *    E.g. If you use a "all" items tag for expiry instead of clear(), that limits you to 4 billion cache items also.
 *
 * @see https://redis.io/topics/lru-cache#eviction-policies Documentation for Redis eviction policies.
 * @see https://redis.io/topics/data-types#sets Documentation for Redis Set datatype.
 *
 * @author Nicolas Grekas <p@tchwork.com>
 * @author André Rømcke <andre.romcke+symfony@gmail.com>
 */
class RedisTagAwareAdapter extends AbstractTagAwareAdapter implements PruneableInterface
{
    use RedisTrait;

    /**
     * On cache items without a lifetime set, we set it to 100 days. This is to make sure cache items are
     * preferred to be evicted over tag Sets, if eviction policy is configured according to requirements.
     */
    private const DEFAULT_CACHE_TTL = 8640000;

    /**
     * @var string|null detected eviction policy used on Redis server
     */
    private $redisEvictionPolicy;
    /**
     * @var string|null detected redis version of Redis server
     */
    private $redisVersion;
    /**
     * @var bool|null Indicate whether this "namespace" has been pruned and what the result was.
     */
    private $pruneResult;
    private $namespace;

    /**
     * @param \Redis|\RedisArray|\RedisCluster|\Predis\ClientInterface|RedisProxy|RedisClusterProxy $redis           The redis client
     * @param string                                                                                $namespace       The default namespace
     * @param int                                                                                   $defaultLifetime The default lifetime
     * @param bool                                                                                  $pruneWithCompression Enable compressed prune. Way more resource intensive.
     */
    public function __construct($redis, string $namespace = '', int $defaultLifetime = 0, MarshallerInterface $marshaller = null, bool $pruneWithCompression = true)
    {
        if ($redis instanceof \Predis\ClientInterface && $redis->getConnection() instanceof ClusterInterface && !$redis->getConnection() instanceof PredisCluster) {
            throw new InvalidArgumentException(sprintf('Unsupported Predis cluster connection: only "%s" is, "%s" given.', PredisCluster::class, get_debug_type($redis->getConnection())));
        }

        if (\defined('Redis::OPT_COMPRESSION') && ($redis instanceof \Redis || $redis instanceof \RedisArray || $redis instanceof \RedisCluster)) {
            $compression = $redis->getOption(\Redis::OPT_COMPRESSION);

            foreach (\is_array($compression) ? $compression : [$compression] as $c) {
                if (\Redis::COMPRESSION_NONE !== $c) {
                    throw new InvalidArgumentException(sprintf('phpredis compression must be disabled when using "%s", use "%s" instead.', static::class, DeflateMarshaller::class));
                }
            }
        }

        $this->init($redis, $namespace, $defaultLifetime, new TagAwareMarshaller($marshaller));
        $this->namespace = $namespace;
        $this->pruneWithCompression = $pruneWithCompression;
    }

    /**
     * {@inheritdoc}
     */
    protected function doSave(array $values, int $lifetime, array $addTagData = [], array $delTagData = []): array
    {
        $eviction = $this->getRedisEvictionPolicy();
        if ('noeviction' !== $eviction && !str_starts_with($eviction, 'volatile-')) {
            throw new LogicException(sprintf('Redis maxmemory-policy setting "%s" is *not* supported by RedisTagAwareAdapter, use "noeviction" or "volatile-*" eviction policies.', $eviction));
        }

        // serialize values
        if (!$serialized = $this->marshaller->marshall($values, $failed)) {
            return $failed;
        }

        // While pipeline isn't supported on RedisCluster, other setups will at least benefit from doing this in one op
        $results = $this->pipeline(static function () use ($serialized, $lifetime, $addTagData, $delTagData, $failed) {
            // Store cache items, force a ttl if none is set, as there is no MSETEX we need to set each one
            foreach ($serialized as $id => $value) {
                yield 'setEx' => [
                    $id,
                    0 >= $lifetime ? self::DEFAULT_CACHE_TTL : $lifetime,
                    $value,
                ];
            }

            // Add and Remove Tags
            foreach ($addTagData as $tagId => $ids) {
                if (!$failed || $ids = array_diff($ids, $failed)) {
                    yield 'sAdd' => array_merge([$tagId], $ids);
                }
            }

            foreach ($delTagData as $tagId => $ids) {
                if (!$failed || $ids = array_diff($ids, $failed)) {
                    yield 'sRem' => array_merge([$tagId], $ids);
                }
            }
        });

        foreach ($results as $id => $result) {
            // Skip results of SADD/SREM operations, they'll be 1 or 0 depending on if set value already existed or not
            if (is_numeric($result)) {
                continue;
            }
            // setEx results
            if (true !== $result && (!$result instanceof Status || Status::get('OK') !== $result)) {
                $failed[] = $id;
            }
        }

        return $failed;
    }

    /**
     * {@inheritdoc}
     */
    protected function doDeleteYieldTags(array $ids): iterable
    {
        $lua = <<<'EOLUA'
            local v = redis.call('GET', KEYS[1])
            local e = redis.pcall('UNLINK', KEYS[1])

            if type(e) ~= 'number' then
                redis.call('DEL', KEYS[1])
            end

            if not v or v:len() <= 13 or v:byte(1) ~= 0x9D or v:byte(6) ~= 0 or v:byte(10) ~= 0x5F then
                return ''
            end

            return v:sub(14, 13 + v:byte(13) + v:byte(12) * 256 + v:byte(11) * 65536)
EOLUA;

        $results = $this->pipeline(function () use ($ids, $lua) {
            foreach ($ids as $id) {
                yield 'eval' => $this->redis instanceof \Predis\ClientInterface ? [$lua, 1, $id] : [$lua, [$id], 1];
            }
        });

        foreach ($results as $id => $result) {
            if ($result instanceof \RedisException || $result instanceof ErrorInterface) {
                CacheItem::log($this->logger, 'Failed to delete key "{key}": '.$result->getMessage(), ['key' => substr($id, \strlen($this->namespace)), 'exception' => $result]);

                continue;
            }

            try {
                yield $id => !\is_string($result) || '' === $result ? [] : $this->marshaller->unmarshall($result);
            } catch (\Exception $e) {
                yield $id => [];
            }
        }
    }

    /**
     * {@inheritdoc}
     */
    protected function doDeleteTagRelations(array $tagData): bool
    {
        $results = $this->pipeline(static function () use ($tagData) {
            foreach ($tagData as $tagId => $idList) {
                array_unshift($idList, $tagId);
                yield 'sRem' => $idList;
            }
        });
        foreach ($results as $result) {
            // no-op
        }

        return true;
    }

    /**
     * {@inheritdoc}
     */
    protected function doInvalidate(array $tagIds): bool
    {
        // This script scans the set of items linked to tag: it empties the set
        // and removes the linked items. When the set is still not empty after
        // the scan, it means we're in cluster mode and that the linked items
        // are on other nodes: we move the links to a temporary set and we
        // garbage collect that set from the client side.

        $lua = <<<'EOLUA'
            redis.replicate_commands()

            local cursor = '0'
            local id = KEYS[1]
            repeat
                local result = redis.call('SSCAN', id, cursor, 'COUNT', 5000);
                cursor = result[1];
                local rems = {}

                for _, v in ipairs(result[2]) do
                    local ok, _ = pcall(redis.call, 'DEL', ARGV[1]..v)
                    if ok then
                        table.insert(rems, v)
                    end
                end
                if 0 < #rems then
                    redis.call('SREM', id, unpack(rems))
                end
            until '0' == cursor;

            redis.call('SUNIONSTORE', '{'..id..'}'..id, id)
            redis.call('DEL', id)

            return redis.call('SSCAN', '{'..id..'}'..id, '0', 'COUNT', 5000)
EOLUA;

        $results = $this->pipeline(function () use ($tagIds, $lua) {
            if ($this->redis instanceof \Predis\ClientInterface) {
                $prefix = $this->redis->getOptions()->prefix ? $this->redis->getOptions()->prefix->getPrefix() : '';
            } elseif (\is_array($prefix = $this->redis->getOption(\Redis::OPT_PREFIX) ?? '')) {
                $prefix = current($prefix);
            }

            foreach ($tagIds as $id) {
                yield 'eval' => $this->redis instanceof \Predis\ClientInterface ? [$lua, 1, $id, $prefix] : [$lua, [$id, $prefix], 1];
            }
        });

        $lua = <<<'EOLUA'
            redis.replicate_commands()

            local id = KEYS[1]
            local cursor = table.remove(ARGV)
            redis.call('SREM', '{'..id..'}'..id, unpack(ARGV))

            return redis.call('SSCAN', '{'..id..'}'..id, cursor, 'COUNT', 5000)
EOLUA;

        $success = true;
        foreach ($results as $id => $values) {
            if ($values instanceof \RedisException || $values instanceof ErrorInterface) {
                CacheItem::log($this->logger, 'Failed to invalidate key "{key}": '.$values->getMessage(), ['key' => substr($id, \strlen($this->namespace)), 'exception' => $values]);
                $success = false;

                continue;
            }

            [$cursor, $ids] = $values;

            while ($ids || '0' !== $cursor) {
                $this->doDelete($ids);

                $evalArgs = [$id, $cursor];
                array_splice($evalArgs, 1, 0, $ids);

                if ($this->redis instanceof \Predis\ClientInterface) {
                    array_unshift($evalArgs, $lua, 1);
                } else {
                    $evalArgs = [$lua, $evalArgs, 1];
                }

                $results = $this->pipeline(function () use ($evalArgs) {
                    yield 'eval' => $evalArgs;
                });

                foreach ($results as [$cursor, $ids]) {
                    // no-op
                }
            }
        }

        return $success;
    }

    /**
     * @TODO Move to RedisTrait? It already has a version check - this would be handy.
     *
     * @return string
     */
    private function getRedisVersion(): string
    {
        if (null !== $this->redisVersion) {
            return $this->redisVersion;
        }

        $hosts = $this->getHosts();
        $host = reset($hosts);
        if ($host instanceof \Predis\Client && $host->getConnection() instanceof ReplicationInterface) {
            // Predis supports info command only on the master in replication environments
            $hosts = [$host->getClientFor('master')];
        }

        foreach ($hosts as $host) {
            $info = $host->info('Server');

            if ($info instanceof ErrorInterface) {
                continue;
            }
            return $this->redisVersion = $info['redis_version'];
        }
        // Fallback to 2.0 like RedisTrait does.
        return $this->redisVersion = '2.0';
    }

    private function getRedisEvictionPolicy(): string
    {
        if (null !== $this->redisEvictionPolicy) {
            return $this->redisEvictionPolicy;
        }

        $hosts = $this->getHosts();
        $host = reset($hosts);
        if ($host instanceof \Predis\Client && $host->getConnection() instanceof ReplicationInterface) {
            // Predis supports info command only on the master in replication environments
            $hosts = [$host->getClientFor('master')];
        }

        foreach ($hosts as $host) {
            $info = $host->info('Memory');

            if ($info instanceof ErrorInterface) {
                continue;
            }

            $info = $info['Memory'] ?? $info;

            return $this->redisEvictionPolicy = $info['maxmemory_policy'];
        }

        return $this->redisEvictionPolicy = '';
    }

    private function getPrefix(): string
    {
        if ($this->redis instanceof \Predis\ClientInterface) {
            $prefix = $this->redis->getOptions()->prefix ? $this->redis->getOptions()->prefix->getPrefix() : '';
        } elseif (\is_array($prefix = $this->redis->getOption(\Redis::OPT_PREFIX) ?? '')) {
            $prefix = current($prefix);
        }

        return $prefix;
    }

    /**
     * Returns all existing tag keys from the cache.
     *
     * @TODO Verify the LUA scripts are redis-cluster safe.
     */
    protected function getAllTagKeys(): array
    {
        $tagKeys = [];
        $prefix = $this->getPrefix();
        // need to trim the \0 for lua script
        $tagsPrefix = trim(self::TAGS_PREFIX);

        // get all SET entries which are tagged
        $getTagsLua = <<<'EOLUA'
            redis.replicate_commands()
            local cursor = ARGV[1]
            local prefix = ARGV[2]
            local tagPrefix = string.gsub(KEYS[1], prefix, "")
            return redis.call('SCAN', cursor, 'COUNT', 5000, 'MATCH', '*' .. tagPrefix .. '*', 'TYPE', 'set')
        EOLUA;
        $cursor = 0;
        do {
            $results = $this->pipeline(function () use ($getTagsLua, $cursor, $prefix, $tagsPrefix) {
                yield 'eval' => [$getTagsLua, [$tagsPrefix, $cursor, $prefix], 1];
            });

            $setKeys = $results->valid() ? iterator_to_array($results) : [];
            // $setKeys[$tagsPrefix] might be an RedisException object -
            // check before just using it.
            if (is_array($setKeys[$tagsPrefix])) {
                [$cursor, $ids] = $setKeys[$tagsPrefix] ?? [null, null];
                // merge the fetched ids together
                $tagKeys = array_merge($tagKeys, $ids);
            } elseif (isset($setKeys[$tagsPrefix]) && $setKeys[$tagsPrefix] instanceof \Throwable) {
                $this->logger->error($setKeys[$tagsPrefix]->getMessage());
            }
        } while ($cursor = (int) $cursor);

        return $tagKeys;
    }


    /**
     * Applies a callback to all tag keys.
     *
     * @TODO Verify the LUA scripts are redis-cluster safe.
     */
    protected function processAllTagKeys(\Closure $generator): \Generator
    {
        $prefix = $this->getPrefix();
        // need to trim the \0 for lua script
        $tagsPrefix = trim(self::TAGS_PREFIX);

        // get all SET entries which are tagged
        $getTagsLua = <<<'EOLUA'
            redis.replicate_commands()
            local cursor = ARGV[1]
            local prefix = ARGV[2]
            local tagPrefix = string.gsub(KEYS[1], prefix, "")
            return redis.call('SCAN', cursor, 'COUNT', 5000, 'MATCH', '*' .. tagPrefix .. '*', 'TYPE', 'set')
        EOLUA;
        $cursor = 0;
        do {
            $results = $this->pipeline(function () use ($getTagsLua, $cursor, $prefix, $tagsPrefix) {
                yield 'eval' => [$getTagsLua, [$tagsPrefix, $cursor, $prefix], 1];
            });
            $setKeys = $results->valid() ? iterator_to_array($results) : [];
            // $setKeys[$tagsPrefix] might be an RedisException object -
            // check before just using it.
            if (is_array($setKeys[$tagsPrefix])) {
                [$cursor, $tagKeys] = $setKeys[$tagsPrefix];
                // merge the fetched ids together
                foreach ($tagKeys as $tagKey) {
                    yield $tagKey => $generator($tagKey);
                }
            } elseif (isset($setKeys[$tagsPrefix]) && $setKeys[$tagsPrefix] instanceof \Throwable) {
                $this->logger->error($setKeys[$tagsPrefix]->getMessage());
            }
        } while ($cursor = (int) $cursor);
    }

    private function processSetMembers(\Closure $generator, $key): \Generator
    {
        // lua for fetching all entries/content from a SET
        $getSetContentLua = <<<'EOLUA'
            redis.replicate_commands()
            local cursor = ARGV[1]
            return redis.call('SSCAN', KEYS[1], cursor, 'COUNT', 5000)
        EOLUA;

        $cursor = 0;
        do {
            // Fetch all referenced cache keys from the tag entry.
            $results = $this->pipeline(function () use ($getSetContentLua, $key, $cursor) {
                yield 'eval' => [$getSetContentLua, [$key, $cursor], 1];
            });
            [$cursor, $setMembers] = $results->valid() ? $results->current() : [null, null];
            yield $cursor => $generator($setMembers);
        } while ($cursor = (int) $cursor);
    }

    /**
     * Accepts a list of cache keys and returns a list with orphaned keys.
     *
     * The method attempts to optimize the testing of the keys by batching the
     * key tests and hence reduce the amount of redis calls.
     *
     * @param array $cacheKeys
     * @param int $chunks Number of chunks to create when processing cacheKeys.
     *
     * @return array
     */
    private function getOrphanedCacheKeys(array $cacheKeys, int $chunks = 2)
    {
        $orphanedCacheKeys = [];
        if (version_compare($this->getRedisVersion(), '3.0.3', '>=')) {
            // If we can check multiple keys at once divide and conquer to have
            // faster execution.
            $cacheKeysChunks = array_chunk($cacheKeys, max(1, floor(count($cacheKeys) / $chunks)), true);
            foreach ($cacheKeysChunks as $cacheKeysChunk) {
                $result = $this->pipeline(function () use ($cacheKeysChunk) {
                    yield 'exists' => [$cacheKeysChunk];
                });
                if ($result->valid()) {
                    $existingKeys = $result->current();
                    if ($existingKeys === 0) {
                        // None of the chunk exists - register all.
                        $orphanedCacheKeys = array_merge($orphanedCacheKeys, $cacheKeysChunk);
                    } elseif ($existingKeys !== ($cacheKeysChunkCount = count($cacheKeysChunk))) {
                        // Some exists some don't - trigger another batch of chunks.
                        // The chunk size should be small enough that the there
                        // is a high possibility to hit an exists run with 0
                        // existing items in it - thus breaking the recursion.
                        // The simplest approach would be to use the ratio
                        // between existing and missing keys - but this doesn't
                        // account for fragmentation. So instead the chunk size
                        // has to be smaller than the ratio in order to get
                        // block hits even if there's fragmentation.
                        // 10 keys, 3 orphans -> chunks min 3, with fragmentation probably more is better.
                        // For now the ratio between existing keys and total keys rounded up to next int plus 1 is used.
                        // This should account for some fragmentation.
                        // @TODO Someone got probabilistic stats on what's the
                        // best chunk size based on keys, orphanedKeys?
                        // @TODO At what chunk size is a single item comparison
                        // more efficient?
                        $newChunks = max(1, ceil($cacheKeysChunkCount / ($cacheKeysChunkCount - $existingKeys)) + 1);
                        $orphanedCacheKeys = array_merge($orphanedCacheKeys, $this->getOrphanedCacheKeys($cacheKeysChunk, $newChunks));
                    }
                }
            }
        } else {
            // Without multi-key support in exists each single reference
            // has to be checked individually to create the delta.
            foreach ($cacheKeys as $cacheKey) {
                $result = $this->pipeline(function () use ($cacheKey) {
                    yield 'exists' => [$cacheKey];
                });
                if ($result->valid() && !$result->current()) {
                    $orphanedCacheKeys[] = $cacheKey;
                }
            }
        }
        return $orphanedCacheKeys;
    }

    /**
     * Checks all tags in the cache for orphaned items and creates a "report" array.
     *
     * @TODO Verify the LUA scripts are redis-cluster safe.
     *
     * @return array{tagKeys: string[], orphanedTagKeys: string[], orphanedTagReferenceKeyCount?: array<string, int>}
     *                                                                                                                 tagKeys: List of all tags in the cache.
     *                                                                                                                 orphanedTagKeys: List of tags that only reference orphaned cache items.
     *                                                                                                                 orphanedTagReferenceKeyCount: Number of orphaned cache item references per tag.
     *                                                                                                                 Keyed by tag, value is the number of orphaned cache item keys.
     */
    private function getOrphanedTagsStats(): array
    {
        // Iterates over all tags, analyzing the keys referencing the tag.
        $tags = $this->processAllTagKeys(function ($tagKey) {
            return $this->processSetMembers(function ($membersChunk) {
                $keyReferencesCount = count($membersChunk);
                // By default assume all references exist.
                $existingKeysCount = count($membersChunk);
                $result = $this->pipeline(function () use ($membersChunk) {
                    yield 'exists' => [$membersChunk];
                });
                if ($result->valid()) {
                    $existingKeysCount = $result->current();
                }
                yield [
                    'keyReferencesCount' => $keyReferencesCount,
                    'existingKeysCount' => $existingKeysCount,
                ];
            }, $tagKey);
        });

        $stats = [
            'tagKeys' => [],
            'orphanedTagKeys' => [],
            'orphanedTagReferenceKeyCount' => [],
        ];
        foreach ($tags as $tag => $referencedKeyChunks) {
            $stats['tagKeys'][$tag] = $tag;
            $orphanedReferencesCount = 0;
            $tagHasExistingKeys = false;
            foreach ($referencedKeyChunks as $referencedKeyChunk) {
                foreach ($referencedKeyChunk as $chunkData) {
                    $tagHasExistingKeys = $tagHasExistingKeys || ($chunkData['existingKeysCount'] > 0);
                    $orphanedReferencesCount += $chunkData['keyReferencesCount'] - $chunkData['existingKeysCount'];
                }
            }
            if (!$tagHasExistingKeys) {
                $stats['orphanedTagKeys'][$tag] = $tag;
            } else {
                $stats['orphanedTagReferenceKeyCount'][$tag] = $orphanedReferencesCount;
            }
        }
        return $stats;
    }

    /**
     * Iterates over all existing tag sets. Checks if the tag set has keys that
     * exist. If none of the references exist anymore the tag is deleted.
     * If a sub-set of references exists and compression is enabled the orphaned
     * references are deleted once the orphan / existing references ratio is
     * above the set threshold.
     *
     * The processing happens progressively to ensure a cleanup happens even in
     * the event of an error somewhere in the later processing.
     *
     * @param bool $compression If enabled orphaned references in tag sets are
     * removed too. Can be quite resource intensive.
     * @param float $compressionRatioThreshold Compression is only executed if
     * at least the given percentage of references of a tag are orphaned.
     * @param int $pruneThrottlingMs Delay in milliseconds between major prune
     * actions. Used to reduce the load on redis during pruning.
     *
     */
    private function pruneOrphanedTags(
        bool $compression = false,
        float $compressionRatioThreshold = 0.3,
        int $pruneThrottlingMs = 50
    ): bool {
        $success = true;
        // Enable compression only if redis version is at least 3.0.3 otherwise
        // the processing has to check each single key instead of batching.
        $compression = $compression && version_compare($this->getRedisVersion(), '3.0.3', '>=');

        // Iterates over all tags, analyzing the keys referencing the tag.
        $tags = $this->processAllTagKeys(function ($tagKey) use ($compression, $pruneThrottlingMs) {
            return $this->processSetMembers(function ($membersChunk) use ($compression, $pruneThrottlingMs) {
                $keyReferencesCount = count($membersChunk);
                // By default assume all references exist.
                $existingKeysCount = count($membersChunk);
                $result = $this->pipeline(function () use ($membersChunk) {
                    yield 'exists' => [$membersChunk];
                });
                if ($result->valid()) {
                    $existingKeysCount = $result->current();
                }
                $returnValue = [
                    'keyReferencesCount' => $keyReferencesCount,
                    'existingKeysCount' => $existingKeysCount,
                ];
                if ($compression) {
                    $returnValue['orphanedKeyReferences'] = $this->getOrphanedCacheKeys($membersChunk);
                }
                usleep($pruneThrottlingMs * 1000);
                yield $returnValue;
            }, $tagKey);
        });

        $orphanedTags = [];
        foreach ($tags as $tag => $referencedKeyChunks) {
            usleep($pruneThrottlingMs * 1000);
            $tagHasExistingKeys = false;
            $tagOrphanedKeyReferences = [];
            $tagKeyReferencesCount = 0;
            foreach ($referencedKeyChunks as $referencedKeyChunk) {
                foreach ($referencedKeyChunk as $chunkData) {
                    // This notation should ensure the existingKeysCount are only
                    // evaluated up until the point $tagHasExistingKeys is true
                    // after that there's no need to eval again.
                    $tagHasExistingKeys = $tagHasExistingKeys || (is_numeric($chunkData['existingKeysCount']) && $chunkData['existingKeysCount'] > 0);
                    $tagKeyReferencesCount += $chunkData['keyReferencesCount'] ?? 0;
                    // Collect the orphaned key references in the tag set.
                    // @TODO If the memory foot print is getting to big we might
                    // have to process the orphans right here or in the actual
                    // generator in order to avoid this collection of keys.
                    // However, collecting them allows for reduction in command
                    // execution.
                    if (!empty($chunkData['orphanedKeyReferences'])) {
                        $tagOrphanedKeyReferences = array_merge($tagOrphanedKeyReferences, $chunkData['orphanedKeyReferences']);
                    }
                }
            }
            if (!$tagHasExistingKeys) {
                $orphanedTags[$tag] = $tag;
            }
            // Delete in batches of 250 to chip away even if there are errors.
            if (count($orphanedTags) >= 250) {
                try {
                    $result = $this->pipeline(function () use ($orphanedTags) {
                        yield 'del' => [$orphanedTags];
                    });
                    if (!$result->valid() || count($orphanedTags) !== $result->current()) {
                        $success = false;
                    }
                } catch (\Throwable $e) {
                    $success = false;
                } finally {
                    $orphanedTags = [];
                }
            }
            // If compression mode is enabled and the count between
            // referenced and existing cache keys differs by more than
            // the compression ratio threshold run the compression.
            // The ratio trigger ensures that a more sensible amount of
            // items is processed. Currently, the set is processed as
            // soon as at least a third of references are orphaned.
            if (
                $tagHasExistingKeys
                && $compression
                && !empty($tagOrphanedKeyReferences)
                && (count($tagOrphanedKeyReferences) / $tagKeyReferencesCount) >= $compressionRatioThreshold
            ) {
                try {
                    // Remove orphaned cache item references from the tag set.
                    // SREM supports multiple members as of redis 2.4.0 - the
                    // compression handling is enabled as of redis 3.0.3.
                    $result = $this->pipeline(function () use ($tag, $tagOrphanedKeyReferences) {
                        yield 'sRem' => array_merge([$tag], $tagOrphanedKeyReferences);
                    });
                    if (!$result->valid() || count($tagOrphanedKeyReferences) !== $result->current()) {
                        $success = false;
                    }
                } catch (\Throwable $e) {
                    $success = false;
                }
            }
        }
        if (!empty($orphanedTags)) {
            $result = $this->pipeline(function () use ($orphanedTags) {
                yield 'del' => [$orphanedTags];
            });
            if (!$result->valid() || 1 !== $result->current()) {
                $success = false;
            }
        }
        return $success;
    }

    public function prune(): bool
    {
        // Only prune once per prune run.
        if (!isset($this->pruneResult)) {
            $this->pruneResult = $this->pruneOrphanedTags($this->pruneWithCompression);
        }
        return $this->pruneResult;
    }
}
