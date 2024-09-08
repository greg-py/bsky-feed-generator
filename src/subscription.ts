import {
  OutputSchema as RepoEvent,
  isCommit,
} from './lexicon/types/com/atproto/sync/subscribeRepos'
import {
  CreateOp,
  FirehoseSubscriptionBase,
  getOpsByType,
} from './util/subscription'
import { Record } from './lexicon/types/app/bsky/feed/post'
import { Database } from './db'
import { PostClassifier } from './classifier'

export class FirehoseSubscription extends FirehoseSubscriptionBase {
  private postClassifier: PostClassifier

  constructor(
    public db: Database,
    public service: string,
    public classifier: PostClassifier,
  ) {
    super(db, service)
    this.postClassifier = classifier
  }

  // Main handler for firehose events
  async handleEvent(evt: RepoEvent) {
    if (!isCommit(evt)) return

    // Retrieve map of operations by type of firehose events
    const ops = await getOpsByType(evt)

    // Determine posts to delete and create in custom feed
    const postsToDelete = ops.posts.deletes.map((del) => del.uri)
    const postsToCreate = this._filterAndMapPostsToCreate(ops.posts.creates)

    // Delete posts marked for deletion from feed (if any)
    if (postsToDelete.length > 0) {
      await this.db
        .deleteFrom('post')
        .where('uri', 'in', postsToDelete)
        .execute()
    }

    // Insert posts marked for creation in feed (if any)
    if (postsToCreate.length > 0) {
      await this.db
        .insertInto('post')
        .values(postsToCreate)
        .onConflict((oc) => oc.doNothing())
        .execute()
    }
  }

  // Logic using keyword list to determine whether
  // post should be included in custom feed
  private _isIncludedInFeed(text: string) {
    const classifications = this.postClassifier.getClassifications(text)

    const topClassification = classifications[0]

    return topClassification && topClassification.label === 'tech'
  }

  // Filters firehose to custom feed posts and maps to DB rows
  private _filterAndMapPostsToCreate(posts: CreateOp<Record>[]) {
    // Filter firehose posts to those that are feed-related
    const feedPosts = posts.filter((post) =>
      this._isIncludedInFeed(post.record.text),
    )

    // Map feed posts to a database row
    const createDbMap = feedPosts.map((post) => {
      return {
        uri: post.uri,
        cid: post.cid,
        indexedAt: new Date().toISOString(),
      }
    })

    return createDbMap
  }
}
