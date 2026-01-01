package lock

func lockHasRecord(flags Flags) bool {
	if flags&FlagNextKey != 0 {
		return true
	}
	if flags&(FlagGap|FlagInsertIntention) != 0 {
		return false
	}
	return true
}

func lockHasGap(flags Flags) bool {
	return flags&(FlagGap|FlagInsertIntention|FlagNextKey) != 0
}

func lockConflict(reqMode Mode, reqFlags Flags, existing *Lock) bool {
	if existing == nil {
		return false
	}
	if lockHasRecord(reqFlags) && lockHasRecord(existing.Flags) {
		if !ModeCompatible(reqMode, existing.Mode) {
			return true
		}
	}
	if lockHasGap(reqFlags) && lockHasGap(existing.Flags) {
		reqInsert := reqFlags&FlagInsertIntention != 0
		existingInsert := existing.Flags&FlagInsertIntention != 0
		if reqInsert && existingInsert {
			return false
		}
		if reqInsert || existingInsert {
			return true
		}
	}
	return false
}
