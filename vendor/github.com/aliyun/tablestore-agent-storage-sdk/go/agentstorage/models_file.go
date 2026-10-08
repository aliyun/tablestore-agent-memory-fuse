package agentstorage

// Memory store modes that expose the item APIs. FileOTS is a read-only
// projection of structured memories; FileMemory stores files as source data.
const (
	StorageModeFileOTS    = "file+ots"
	StorageModeFileMemory = "filemem"
)

// ItemTypeMemoryFile is the only item type accepted by the item APIs. The
// SDK fills it into any item request whose Type field is empty.
const ItemTypeMemoryFile = "memoryfile"

// ItemTypeMemoryFilePrefix is the directory-marker type emitted by ListItems
// when Depth is 1 and a deeper subtree is rolled up. Prefix entries carry
// only Scope and Path.
const ItemTypeMemoryFilePrefix = "memoryfile_prefix"

// Service limits documented for item APIs.
const (
	// MaxItemContentBytes is the maximum item content size accepted by the
	// service, in bytes (100 KiB). Oversized content fails with 413
	// PAYLOAD_TOO_LARGE.
	MaxItemContentBytes = 102400

	// MaxItemPathBytes is the maximum normalized item path length, in bytes.
	MaxItemPathBytes = 1024

	// MaxItemsPerScope is the conventional per-mount-unit (store + exact
	// scope) file-count cap. Whether the service enforces it is deployment
	// configuration; when enforced, exceeding it fails with 409
	// QUOTA_EXCEEDED.
	MaxItemsPerScope = 2000
)

// ItemOperation is the operation recorded by an item version.
type ItemOperation string

const (
	ItemOperationCreated  ItemOperation = "created"
	ItemOperationModified ItemOperation = "modified"
	ItemOperationDeleted  ItemOperation = "deleted"
)

// ItemView selects how much payload a response carries. ItemViewBasic (the
// default when omitted) omits content; ItemViewFull includes it.
type ItemView string

const (
	ItemViewBasic ItemView = "basic"
	ItemViewFull  ItemView = "full"
)

// ItemActor is the typed actor attribution on version rows. ActorType is
// "session", "api_key" or "user"; ActorID is the corresponding identifier.
type ItemActor struct {
	ActorType string `json:"actorType"`
	ActorID   string `json:"actorId"`
}

// ItemScopedRequest carries the item type, store and exact four-part scope
// shared by all eight item actions: AddItem, ListItems, GetItem, UpdateItem,
// DeleteItem, ListItemVersions, GetItemVersion and RedactItemVersion.
//
// Item APIs require a writable filemem store. On a file+ots read-only
// projection, writes fail with 409 READ_ONLY_STORE and the version APIs fail
// with 400; on a plain ots store every item action fails with 400.
//
// Scope rules per action family:
//
//   - Write actions (AddItem, UpdateItem, single DeleteItem) require all
//     four segments (appId, tenantId, agentId, runId) exact - the "*"
//     wildcard is rejected with 400.
//   - Read actions (ListItems, ListItemVersions) and ranged DeleteItem
//     allow the left-prefix wildcard: appId/tenantId are required and
//     exact, agentId/runId may be "*", and once a segment is "*" every
//     segment after it must be "*" too (or omitted).
//   - GetItemVersion and RedactItemVersion require all four segments
//     exact, like the write actions: the server derives item ownership
//     from the versionId and verifies it against the exact scope.
//
// Each scope field is capped at 128 bytes and must not contain "/" or NUL.
type ItemScopedRequest struct {
	// Type discriminates the item type. Required by the service on all
	// eight actions, and "memoryfile" is its only accepted value - the SDK
	// fills it in when the caller leaves it empty.
	Type            string `json:"type"`
	MemoryStoreName string `json:"memoryStoreName"`
	Scope           Scope  `json:"scope"`
}

// setDefaultType fills Type with ItemTypeMemoryFile when unset.
func (r *ItemScopedRequest) setDefaultType() {
	if r.Type == "" {
		r.Type = ItemTypeMemoryFile
	}
}

// Path rules, enforced by the service:
//
//   - must start with "/" - the service no longer adds a missing leading "/"
//   - at most MaxItemPathBytes bytes after NFC normalization
//   - "." and ".." segments, empty segments ("//"), a bare "/", a trailing
//     "/", NUL characters and invalid UTF-8 are all rejected with 400
//   - any valid UTF-8 is otherwise allowed, including CJK; matching is
//     byte-exact and case-sensitive

// AddItemRequest is the request for AddItem.
type AddItemRequest struct {
	ItemScopedRequest
	Path string `json:"path"`
	// Content is UTF-8 text, at most MaxItemContentBytes bytes - oversized
	// content fails with 413 PAYLOAD_TOO_LARGE and invalid UTF-8 with 400.
	// Required; an empty string creates an empty file.
	Content string `json:"content"`
	// SessionID is recorded as the version's session actor.
	SessionID string `json:"sessionId,omitempty"`
	// View defaults to ItemViewBasic (response without content);
	// ItemViewFull echoes content.
	View ItemView `json:"view,omitempty"`
}

// ListItemsRequest is the request for ListItems. The scope may use the
// left-prefix wildcard (see ItemScopedRequest).
type ListItemsRequest struct {
	ItemScopedRequest
	// PathPrefix is a directory-style prefix. It must be segment-aligned:
	// start and end with "/" (a bare "/" lists the whole tree, same as
	// omitting it). "."/".." segments, NUL and control/format characters
	// are rejected with 400.
	PathPrefix string `json:"pathPrefix,omitempty"`
	// Depth 0 (the default when nil) lists the whole subtree recursively;
	// 1 lists only direct children and rolls deeper directories up into
	// entries typed ItemTypeMemoryFilePrefix. Any other value is rejected
	// with 400.
	Depth *int `json:"depth,omitempty"`
	// View defaults to ItemViewBasic; ItemViewFull includes each file's
	// content.
	View ItemView `json:"view,omitempty"`
	// Limit defaults to 20; maximum 100, and capped at 20 when View is
	// ItemViewFull.
	Limit int `json:"limit,omitempty"`
	// NextToken is an opaque cursor. It must be replayed with the same
	// PathPrefix that produced it; a cursor outside that prefix is
	// rejected with 400.
	NextToken string `json:"nextToken,omitempty"`
}

// ItemPrecondition requires the addressed item to have the specified latest
// version. A mismatch is rejected by the service as a concurrent update.
type ItemPrecondition struct {
	ExpectedVersionID string `json:"expectedVersionId"`
}

// GetItemRequest is the request for GetItem. Exactly one of ItemID and Path
// must be set.
type GetItemRequest struct {
	ItemScopedRequest
	ItemID string `json:"itemId,omitempty"`
	Path   string `json:"path,omitempty"`
	// View defaults to ItemViewBasic - metadata only. ItemViewFull
	// includes content. It replaces the removed includeContent flag; note
	// the default is inverted relative to the old includeContent=true
	// default.
	View ItemView `json:"view,omitempty"`
}

// UpdateItemRequest is the request for UpdateItem. ItemID is required and is
// the only address of the existing item. Content rewrites the item, while Path
// is an optional rename target; at least one of Content and Path must be set,
// and they may be combined in one atomic update. Overwrite may be true only
// when Path is set.
type UpdateItemRequest struct {
	ItemScopedRequest
	ItemID       string            `json:"itemId"`
	Content      *string           `json:"content,omitempty"`
	Path         *string           `json:"path,omitempty"`
	Precondition *ItemPrecondition `json:"precondition,omitempty"`
	Overwrite    bool              `json:"overwrite,omitempty"`
	SessionID    string            `json:"sessionId,omitempty"`
	View         ItemView          `json:"view,omitempty"`
}

// DeleteItemRequest is the request for DeleteItem, in one of two modes:
//
//   - Exact delete: exact full scope plus exactly one of ItemID and Path,
//     with an optional resource-version Precondition. Synchronous; the
//     response carries Deleted=true.
//   - Ranged delete: wildcard scope (left-prefix rule, appId/tenantId
//     required). Asynchronous - the service answers 200 with a TaskID
//     identifying the sharded delete chain (it used to answer 202; the
//     accept status moved to 200 because the OTS gateway only forwards
//     200 responses). ItemID, Path and Precondition must all be omitted;
//     PathPrefix optionally narrows the range (segment-aligned, see
//     ListItemsRequest.PathPrefix).
type DeleteItemRequest struct {
	ItemScopedRequest
	ItemID       string            `json:"itemId,omitempty"`
	Path         string            `json:"path,omitempty"`
	PathPrefix   string            `json:"pathPrefix,omitempty"`
	Precondition *ItemPrecondition `json:"precondition,omitempty"`
	SessionID    string            `json:"sessionId,omitempty"`
}

// ListItemVersionsRequest is the request for ListItemVersions - a
// scope-level version listing ordered by createdAt descending. All filters
// are optional; the scope may use the left-prefix wildcard (see
// ItemScopedRequest).
type ListItemVersionsRequest struct {
	ItemScopedRequest
	// ItemID is an optional filter; when omitted, versions of every item
	// in scope match.
	ItemID string `json:"itemId,omitempty"`
	// SessionID is an actor filter; mutually exclusive with ApiKeyID.
	SessionID string `json:"sessionId,omitempty"`
	// ApiKeyID is an actor filter; mutually exclusive with SessionID.
	// Note: the service currently rejects a non-empty apiKeyId with 400
	// (api-key actors cannot be derived until authentication context is
	// wired into item mutations); the field is kept for wire compatibility.
	ApiKeyID string `json:"apiKeyId,omitempty"`
	// CreatedAtGte is an inclusive RFC3339 lower bound on createdAt.
	CreatedAtGte string `json:"createdAtGte,omitempty"`
	// CreatedAtLte is an inclusive RFC3339 upper bound on createdAt.
	CreatedAtLte string `json:"createdAtLte,omitempty"`
	// Operation is case-sensitive; any other value is rejected with 400.
	Operation ItemOperation `json:"operation,omitempty"`
	// View defaults to ItemViewBasic; ItemViewFull includes each version's
	// content.
	View ItemView `json:"view,omitempty"`
	// Limit defaults to 20; maximum 100, and capped at 20 when View is
	// ItemViewFull.
	Limit     int    `json:"limit,omitempty"`
	NextToken string `json:"nextToken,omitempty"`
}

// GetItemVersionRequest is the request for GetItemVersion. VersionID is
// self-contained: the server derives all addressing from it and verifies
// ownership against the exact four-part scope (wildcards are rejected).
type GetItemVersionRequest struct {
	ItemScopedRequest
	VersionID string `json:"versionId"`
	// View defaults to ItemViewBasic; ItemViewFull includes content (never
	// on redacted versions).
	View ItemView `json:"view,omitempty"`
}

// RedactItemVersionRequest is the request for RedactItemVersion; the scope
// must be the exact four-part scope owning the version. It irreversibly
// redacts a single historical version snapshot: content, path,
// contentSha256 and contentSizeBytes are cleared while operation, createdAt
// and the original createdBy are kept. The current item state and latestSeq
// are untouched and no new version row is created.
//
// Redaction is idempotent and first-wins: repeating it returns 200 with the
// existing redactedAt/redactedBy. Afterwards GetItemVersion still returns
// 200 for the version but never returns its content.
type RedactItemVersionRequest struct {
	ItemScopedRequest
	VersionID string `json:"versionId"`
	// SessionID is recorded as the session actor in redactedBy on the
	// first redaction.
	SessionID string `json:"sessionId,omitempty"`
}

// Item is the current state of one item. Content is nil when the response
// omits it (the default basic view); an empty file has Content pointing at
// "". Type echoes ItemTypeMemoryFile in single-item responses.
type Item struct {
	Type             string  `json:"type,omitempty"`
	ItemID           string  `json:"itemId"`
	Scope            Scope   `json:"scope"`
	Path             string  `json:"path"`
	Content          *string `json:"content,omitempty"`
	ContentSHA256    string  `json:"contentSha256"`
	ContentSizeBytes int64   `json:"contentSizeBytes"`
	LatestSeq        int64   `json:"latestSeq"`
	// LatestVersionID is the head-version pointer.
	LatestVersionID string `json:"latestVersionId"`
	CreatedAt       string `json:"createdAt"`
	UpdatedAt       string `json:"updatedAt"`
}

// ItemListEntry is one ListItems entry. File entries carry Type
// ItemTypeMemoryFile plus the item fields; directory roll-ups emitted with
// Depth 1 carry Type ItemTypeMemoryFilePrefix and only Scope and Path.
type ItemListEntry struct {
	Type             string  `json:"type"`
	ItemID           string  `json:"itemId,omitempty"`
	Scope            Scope   `json:"scope"`
	Path             string  `json:"path"`
	Content          *string `json:"content,omitempty"`
	ContentSHA256    string  `json:"contentSha256,omitempty"`
	ContentSizeBytes int64   `json:"contentSizeBytes,omitempty"`
	LatestSeq        int64   `json:"latestSeq,omitempty"`
	LatestVersionID  string  `json:"latestVersionId,omitempty"`
	CreatedAt        string  `json:"createdAt,omitempty"`
	UpdatedAt        string  `json:"updatedAt,omitempty"`
}

// IsPrefix reports whether the entry is a directory roll-up marker.
func (e *ItemListEntry) IsPrefix() bool { return e.Type == ItemTypeMemoryFilePrefix }

// ItemVersion is one immutable item version. Content is nil when the
// response omits it (the default basic view, or redacted versions); an
// empty version has Content pointing at "". The redaction fields appear
// only after RedactItemVersion.
type ItemVersion struct {
	Type             string        `json:"type,omitempty"`
	VersionID        string        `json:"versionId"`
	ItemID           string        `json:"itemId"`
	Scope            Scope         `json:"scope"`
	Operation        ItemOperation `json:"operation"`
	Path             string        `json:"path"`
	Content          *string       `json:"content,omitempty"`
	ContentSHA256    string        `json:"contentSha256"`
	ContentSizeBytes int64         `json:"contentSizeBytes"`
	// CreatedBy is the typed actor that wrote this version.
	CreatedBy *ItemActor `json:"createdBy,omitempty"`
	CreatedAt string     `json:"createdAt"`
	Redacted  bool       `json:"redacted,omitempty"`
	// RedactedAt is the time of the first redaction. Present only once
	// redacted.
	RedactedAt string `json:"redactedAt,omitempty"`
	// RedactedBy is the typed actor of the first redaction. Present only
	// once redacted.
	RedactedBy *ItemActor `json:"redactedBy,omitempty"`
}

type (
	AddItemResponse           = Item
	GetItemResponse           = Item
	UpdateItemResponse        = Item
	GetItemVersionResponse    = ItemVersion
	RedactItemVersionResponse = ItemVersion
)

// DeleteItemResponse covers both DeleteItem outcomes: a synchronous single
// delete carries ItemID and Deleted=true; an asynchronous ranged delete
// carries TaskID and Status="pending" (also HTTP 200).
type DeleteItemResponse struct {
	Type    string `json:"type"`
	ItemID  string `json:"itemId,omitempty"`
	Deleted bool   `json:"deleted,omitempty"`
	TaskID  string `json:"taskId,omitempty"`
	Status  string `json:"status,omitempty"`
}

// IsRanged reports whether the response came from an asynchronous ranged
// delete.
func (r *DeleteItemResponse) IsRanged() bool { return r.TaskID != "" }

// ListItemsResponse is the response of ListItems. ReadOnly is true for
// file+ots projections and omitted for writable stores.
type ListItemsResponse struct {
	Type      string          `json:"type"`
	Items     []ItemListEntry `json:"items"`
	NextToken string          `json:"nextToken,omitempty"`
	ReadOnly  bool            `json:"readOnly,omitempty"`
}

// ListItemVersionsResponse is the response of ListItemVersions.
type ListItemVersionsResponse struct {
	Type      string        `json:"type"`
	Versions  []ItemVersion `json:"versions"`
	NextToken string        `json:"nextToken,omitempty"`
}
