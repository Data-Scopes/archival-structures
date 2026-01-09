import time
from multiprocessing import Pool
from pathlib import Path

from archival_structures.image.image_processing import make_image_thumbnail


def do_task(task):
    make_image_thumbnail(task['image_fp'], task['thumb_dir'], thumb_width=task['thumb_width'])


def main():
    archive_dir = Path('/Volumes/T7_Data/Data/PageXML/Stadsarchief_Amsterdam/')
    filepaths = [filepath for filepath in archive_dir.glob('**/*')]
    image_fps = [fp for fp in filepaths if fp.name.endswith('.jpg')]
    thumb_width = 600
    thumb_dir = archive_dir.joinpath('thumbnails')

    if not thumb_dir.exists():
        thumb_dir.mkdir()

    tasks = []
    for image_fp in image_fps:
        task = {'image_fp': image_fp, 'thumb_dir': thumb_dir, 'thumb_width': thumb_width}
        tasks.append(task)

    start = time.time()
    pool = Pool(8)
    pool.map(do_task, tasks)
    end = time.time()
    print(f"took {end - start: >.1f} seconds for {len(tasks)} tasks.")


if __name__ == "__main__":
    main()