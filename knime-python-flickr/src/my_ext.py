import knime.extension as knext


main_category = knext.category(
    path="/community/",
    level_id="flickr_img",
    name="Flickr Images Retrieval",
    description="Nodes for Flickr Images Retrieval",
    icon="icons/icon.png",
)

from nodes import flickr_image_downloader