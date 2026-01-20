package store

// TODO - Add lock if necessary in the future
type storeKeyDir map[string]storeKeyInfo

type storeKeyInfo struct {
	file_id   uint64
	value_sz  int
	value_pos int
	tstamp    uint32
}

func (keydir storeKeyDir) getInfo(key string) (storeKeyInfo, bool) {
	info, ok := keydir[key]
	return info, ok
}

func (keydir storeKeyDir) putInfo(key string, info storeKeyInfo) {
	keydir[key] = info
}

func (keydir storeKeyDir) removeInfo(key string) {
	delete(keydir, key)
}

func (keydir storeKeyDir) getKeys() []string {
	keys := []string{}
	for key := range keydir {
		keys = append(keys, key)
	}
	return keys
}
