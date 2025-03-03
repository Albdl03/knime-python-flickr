import requests
import knime.extension as knext
import pandas as pd
import logging
from PIL import Image
from utils import knutills as kutil
LOGGER = logging.getLogger(__name__)
import io

@knext.node(
    name="Flickr Image Downloader",
    node_type=knext.NodeType.SOURCE,
    icon_path="icons/flickr.png",
    category=kutil.main_category,
    id="img-downloader",
)
@knext.output_table(
    name="Fetched Images",
    description="Table containing the URLs of the images downloaded from Flickr.",
)
class FlickrImageDownloader:
    """
    Flickr Image Downloader

    The Flickr Image Downloader Node downloads images from Flickr based on a specified search term. 
    It uses the Flickr API to search for images and retrieve their URLs. 
    The node allows users to specify the number of images to download and requires a Flickr API key for authentication. 
    The downloaded images are then returned as a KNIME table containing the image.

    Key functionalities:

    -   Configuring the node with the following parameters: Flickr API key, search term, and number of images to download.
    -   Fetching image URLs from Flickr using its API.
    -   Handling pagination to retrieve the specified number of images (the maximum number of images per page is 500).
    -   Validating and filtering the retrieved image metadata.
    -   Downloading the images and converting them to PNG.
    -   Returning the images as a KNIME table.

    """

    credential_param = knext.StringParameter(
        label="Flickr API Key",
        description="The credentials containing the Flickr API key in its *password* field (the *username* is ignored).",
        choices=lambda a: knext.DialogCreationContext.get_credential_names(a),
    )

    search_term = knext.StringParameter(
        label="Search Term",
        description="Search term for the images to download.",
        default_value="",
    )

    num_images = knext.IntParameter(
        label="Number of Images",
        description="Number of images to retrieve from Flickr between 1 and 20000.",
        default_value=10,
        min_value=1,
        max_value=20000,
    )

    def configure(self, ctx: knext.ConfigurationContext):
        if not ctx.get_credential_names():
            raise knext.InvalidParametersError("No credentials provided.")
        if not self.credential_param:
            raise knext.InvalidParametersError("Credentials not selected.")
        return knext.Column(name="Image", ktype=knext.logical(Image.Image))

    def execute(self, ctx: knext.ExecutionContext):
        credentials = ctx.get_credentials(self.credential_param)
        self.api_key = (
            credentials.password
        )
        base_url = "https://www.flickr.com/services/rest/"

        max_per_page = 500  # Flickr's per-page limit
        num_requested_images = self.num_images  # User-requested total images
        urls = []
        page = 1

        while len(urls) < num_requested_images:
            num_remaining_images = num_requested_images - len(urls)
            per_page = min(num_remaining_images, max_per_page)  # Limit per request

            params = {
                "method": "flickr.photos.search",
                "api_key": self.api_key,
                "text": self.search_term,
                "per_page": per_page,
                "page": page,
                "format": "json",
                "nojsoncallback": 1,
            }

            response = requests.get(base_url, params=params)

            if response.status_code != 200:
                raise RuntimeError(
                    f"Failed to fetch data from Flickr API: {response.text}"
                )

            # TODO is the following comment correct? Then delete this TODO
            # data contains the entire page, of which we try to obtain up to 500 photos
            data = response.json()

            if "photos" not in data or "photo" not in data["photos"]:
                raise ValueError("Unexpected API response format.")

            photo_list = data["photos"]["photo"]

            if not photo_list:
                LOGGER.info("No more images available. Stopping early.")
                break  # Stop if no more images available

            for photo in photo_list:
                # Validate metadata
                if (
                    "farm" not in photo
                    or photo["farm"] == 0
                    or "server" not in photo
                    or not photo["server"]
                    or "id" not in photo
                    or not photo["id"]
                    or "secret" not in photo
                    or not photo["secret"]
                ):
                    LOGGER.info(f"Skipping invalid photo: {photo}")
                    continue  # Skip invalid entries

                photo_url = f"https://farm{photo['farm']}.staticflickr.com/{photo['server']}/{photo['id']}_{photo['secret']}.jpg"

                # TODO Do we really need the following if statement? Will there really be duplicates? This is a very expensive computation. If not needed, delete.
                if photo_url not in urls:  # Prevent duplicates
                    urls.append(photo_url)

                if len(urls) >= num_requested_images:
                    break  # Stop if the amount of requested images is reached

            page += 1  # Move to the next page

        LOGGER.debug(f"Retrieved {len(urls)} unique image URLs from Flickr.")

        # Create a DataFrame with the image URLs
        df = pd.DataFrame()
        df["Image"] = [self.__open_image_from_url(i) for i in urls]

        ctx.set_progress(0.9, "Image retrieval complete.")
        return knext.Table.from_pandas(df)

    def __open_image_from_url(self, url):
        response = requests.get(url)
        if response.status_code == 200:
            img = Image.open(io.BytesIO(response.content))
            buffer = io.BytesIO()
            img.save(buffer, format="PNG")
            buffer.seek(0)
            return Image.open(buffer)
        else: # TODO Future work: consider adding a checkbox to the config dialog to allow omitting this error-raising and just logging non-functioning URLs.
            raise ValueError(f"Failed to fetch image from URL: {url}")