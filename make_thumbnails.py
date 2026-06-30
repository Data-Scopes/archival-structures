import shutil
import time
from multiprocessing import Pool
from pathlib import Path

from archival_structures.image.image_processing import make_image_thumbnail


def make_thumbnail_filename(image_filepath: Path, thumb_dir: Path, thumb_width: int = 500,
                            image_format: str = 'png'):
    inventory_id = image_filepath.parts[-2]
    archive_id = '_'.join(inventory_id.split('_')[:-1])
    if not thumb_dir.exists:
        print(f"creating thumb_dir {thumb_dir}")
        thumb_dir.mkdir()
    archive_dir = thumb_dir.joinpath(archive_id)
    if not archive_dir.exists:
        print(f"creating archive_dir {archive_dir}")
        archive_dir.mkdir()
    inventory_dir = archive_dir.joinpath(inventory_id)
    if not inventory_dir.exists:
        print(f"creating inventory_dir {inventory_dir}")
        inventory_dir.mkdir()
    file_name = f"thumb-width_{thumb_width}-scan-{image_filepath.name}.{image_format}"
    return inventory_dir.joinpath(file_name)


def do_task(task):
    make_image_thumbnail(task['image_fp'], task['thumb_fp'], thumb_width=task['thumb_width'])


def main():
    archive_dir = Path('/Volumes/T7_Data/Data/PageXML/Stadsarchief_Amsterdam/')
    thumb_dir = archive_dir.joinpath('thumbnails')
    filepaths = [filepath for filepath in archive_dir.glob('**/*')]
    image_fps = [fp for fp in filepaths if fp.name.endswith('.jpg')]
    thumb_width = 600

    archive_dir = Path('data/downloads/NL-AsnDA_0114.11/NL-AsnDA_0114.11_1')
    thumb_dir = Path('data/thumbs/')
    image_fps = [filepath for filepath in archive_dir.glob('*.jp2')]

    thumb_width = 300

    tasks = []
    for image_fp in image_fps:
        thumb_fp = make_thumbnail_filename(image_fp, thumb_dir, thumb_width)
        if thumb_fp.exists():
            print(f"skipping {thumb_fp} - already exists")
            continue
        task = {'image_fp': image_fp, 'thumb_fp': thumb_fp, 'thumb_width': thumb_width}
        tasks.append(task)

    start = time.time()
    pool = Pool(8)
    pool.map(do_task, tasks)
    end = time.time()
    print(f"took {end - start: >.1f} seconds for {len(tasks)} tasks.")


if __name__ == "__main__":
    main()