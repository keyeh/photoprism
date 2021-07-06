package entity

import (
	"sync"

	"github.com/jinzhu/gorm"
	"github.com/photoprism/photoprism/pkg/rnd"
)

var photoMergeMutex = sync.Mutex{}

// ResolvePrimary ensures there is only one primary file for a photo.
func (m *Photo) ResolvePrimary() error {
	var file File

	if err := Db().Where("file_primary = 1 AND photo_id = ?", m.ID).First(&file).Error; err == nil && file.ID > 0 {
		return file.ResolvePrimary()
	}

	return nil
}

// Identical returns identical photos that can be merged.
func (m *Photo) Identical(includeMeta, includeUuid bool) (identical Photos, err error) {
	if m.PhotoStack == IsUnstacked || m.PhotoName == "" {
		return identical, nil
	}

	log.Debugf("LOOKING FOR VISUALLY SIMILAR IMAGES")
	var myfile File // get this photo's file
	if err := Db().Where("photo_id = ?", m.ID).First(&myfile).Error; err != nil {
		log.Error("DB ERROR IN FILE QUERY!")
	} else if err := Db().
		// find photos with similar files, using file diff and color hamming distance
		Where(`id IN (
			SELECT photo_id from (
				SELECT
					photo_id,
					original_name,
					file_colors,
					BIT_COUNT(CONV(file_colors,16,10) ^ CONV(?,16,10)) as color_hamming
				FROM files
				WHERE file_diff BETWEEN ? AND ?
			) as f
			WHERE f.color_hamming < 5
		)`,
			myfile.FileColors, myfile.FileDiff-2, myfile.FileDiff+2).
		Order("photo_quality DESC, id ASC").Find(&identical).Error; err != nil {
		log.Error("DB ERROR IN HAMMING DIFF QUERY!")
	} else if len(identical) > 1 {
		log.Debug("FOUND VISUALLY SIMILAR IMAGE ALREADY EXISTS")
		return identical, nil
	} else {
		log.Debug("NO VIUSALLY SIMILAR IMAGES FOUND")
	}

	switch {
	case includeMeta && includeUuid && m.HasLocation() && m.TakenSrc == SrcMeta && rnd.IsUUID(m.UUID):
		if err := Db().
			Where("(taken_at = ? AND taken_src = 'meta' AND photo_stack > -1 AND cell_id = ? AND camera_serial = ? AND camera_id = ?) "+
				"OR (uuid = ? AND photo_stack > -1)"+
				"OR (photo_path = ? AND photo_name = ?)",
				m.TakenAt, m.CellID, m.CameraSerial, m.CameraID, m.UUID, m.PhotoPath, m.PhotoName).
			Order("photo_quality DESC, id ASC").Find(&identical).Error; err != nil {
			return identical, err
		}
	case includeMeta && m.HasLocation() && m.TakenSrc == SrcMeta:
		if err := Db().
			Where("(taken_at = ? AND taken_src = 'meta' AND photo_stack > -1 AND cell_id = ? AND camera_serial = ? AND camera_id = ?) "+
				"OR (photo_path = ? AND photo_name = ?)",
				m.TakenAt, m.CellID, m.CameraSerial, m.CameraID, m.PhotoPath, m.PhotoName).
			Order("photo_quality DESC, id ASC").Find(&identical).Error; err != nil {
			return identical, err
		}
	case includeUuid && rnd.IsUUID(m.UUID):
		if err := Db().
			Where("(uuid = ? AND photo_stack > -1) OR (photo_path = ? AND photo_name = ?)",
				m.UUID, m.PhotoPath, m.PhotoName).
			Order("photo_quality DESC, id ASC").Find(&identical).Error; err != nil {
			return identical, err
		}
	default:
		if err := Db().
			Where("photo_path = ? AND photo_name = ?", m.PhotoPath, m.PhotoName).
			Order("photo_quality DESC, id ASC").Find(&identical).Error; err != nil {
			return identical, err
		}
	}

	return identical, nil
}

// Merge photo with identical ones.
func (m *Photo) Merge(mergeMeta, mergeUuid bool) (original Photo, merged Photos, err error) {
	photoMergeMutex.Lock()
	defer photoMergeMutex.Unlock()

	Db().LogMode(true)
	identical, err := m.Identical(mergeMeta, mergeUuid)
	Db().LogMode(false)

	if len(identical) < 2 || err != nil {
		return Photo{}, merged, err
	}

	logResult := func(res *gorm.DB) {
		if res.Error != nil {
			log.Errorf("merge: %s", res.Error.Error())
			err = res.Error
		}
	}

	for i, merge := range identical {
		if i == 0 {
			original = merge
			log.Debugf("photo: merging id %d with %d identical", original.ID, len(identical)-1)
			continue
		}

		deleted := Timestamp()

		logResult(UnscopedDb().Exec("UPDATE `files` SET photo_id = ?, photo_uid = ?, file_primary = 0 WHERE photo_id = ?", original.ID, original.PhotoUID, merge.ID))
		logResult(UnscopedDb().Exec("UPDATE `photos` SET photo_quality = -1, deleted_at = ? WHERE id = ?", Timestamp(), merge.ID))

		switch DbDialect() {
		case MySQL:
			logResult(UnscopedDb().Exec("UPDATE IGNORE `photos_keywords` SET `photo_id` = ? WHERE photo_id = ?", original.ID, merge.ID))
			logResult(UnscopedDb().Exec("UPDATE IGNORE `photos_labels` SET `photo_id` = ? WHERE photo_id = ?", original.ID, merge.ID))
			logResult(UnscopedDb().Exec("UPDATE IGNORE `photos_albums` SET `photo_uid` = ? WHERE photo_uid = ?", original.PhotoUID, merge.PhotoUID))
		case SQLite:
			logResult(UnscopedDb().Exec("UPDATE OR IGNORE `photos_keywords` SET `photo_id` = ? WHERE photo_id = ?", original.ID, merge.ID))
			logResult(UnscopedDb().Exec("UPDATE OR IGNORE `photos_labels` SET `photo_id` = ? WHERE photo_id = ?", original.ID, merge.ID))
			logResult(UnscopedDb().Exec("UPDATE OR IGNORE `photos_albums` SET `photo_uid` = ? WHERE photo_uid = ?", original.PhotoUID, merge.PhotoUID))
		default:
			log.Warnf("merge: unknown sql dialect")
		}

		merge.DeletedAt = &deleted
		merge.PhotoQuality = -1

		merged = append(merged, merge)
	}

	if original.ID != m.ID {
		deleted := Timestamp()
		m.DeletedAt = &deleted
		m.PhotoQuality = -1
	}

	return original, merged, err
}
