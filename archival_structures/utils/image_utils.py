from pathlib import Path

from PIL import Image


def convert_image(orig_image, converted_image):
    im = Image.open(orig_image)
    im.convert("RGB").save(converted_image,
                           #'JPEG',
                           'PNG',
                           # quality_mode='dB',
                           # quality_layers=[41]
                           )


def main():
    thumb_dir = Path("../../data/thumbs/NL-AsnDA_0114.11")
    jp2_files = list(thumb_dir.glob('*.jp2'))
    print(f"number of jp2 files: {len(jp2_files)}")
    for ti, tf_jp2 in enumerate(jp2_files):
        # tf_jpg = thumb_dir / tf_jp2.name.replace('.jp2', '.jpg')
        # print(f"{ti} {tf_jp2} {tf_jpg}")
        # convert_image(orig_image=tf_jp2, converted_image=tf_jpg)
        tf_png = thumb_dir / tf_jp2.name.replace('.jp2', '.png')
        print(f"{ti} {tf_jp2} {tf_png}")
        convert_image(orig_image=tf_jp2, converted_image=tf_png)


if __name__ == "__main__":
    main()
