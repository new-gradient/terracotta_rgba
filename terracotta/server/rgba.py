"""server/rgba.py

Flask route to handle /rgba calls.
"""

from typing import Optional, Any, Mapping, Dict, Tuple
import json

from marshmallow import Schema, fields, validate, pre_load, ValidationError, EXCLUDE
from flask import request, send_file, Response

from terracotta.server.fields import StringOrNumber, validate_stretch_range
from terracotta.server.flask_api import TILE_API

import logging


class RGBAQuerySchema(Schema):
    keys = fields.String(
        required=True, description="Keys identifying dataset, in order"
    )
    tile_z = fields.Int(required=True, description="Requested zoom level")
    tile_y = fields.Int(required=True, description="y coordinate")
    tile_x = fields.Int(required=True, description="x coordinate")


class RGBAOptionSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    r = fields.String(required=True, description="Key value for red band")
    g = fields.String(required=True, description="Key value for green band")
    b = fields.String(required=True, description="Key value for blue band")
    a = fields.String(required=True, description="Key value for alpha band")
    r_range = fields.List(
        StringOrNumber(allow_none=True, validate=validate_stretch_range),
        validate=validate.Length(equal=2),
        example="[0,1]",
        missing=None,
        description=(
            "Stretch range [min, max] to use for the red band as JSON array. "
            "Min and max may be numbers to use as absolute range, or strings "
            "of the format `p<digits>` with an integer between 0 and 100 "
            "to use percentiles of the image instead. "
            "Null values indicate global minimum / maximum."
        ),
    )
    g_range = fields.List(
        StringOrNumber(allow_none=True, validate=validate_stretch_range),
        validate=validate.Length(equal=2),
        example="[0,1]",
        missing=None,
        description=(
            "Stretch range [min, max] to use for the green band as JSON array. "
            "Min and max may be numbers to use as absolute range, or strings "
            "of the format `p<digits>` with an integer between 0 and 100 "
            "to use percentiles of the image instead. "
            "Null values indicate global minimum / maximum."
        ),
    )
    b_range = fields.List(
        StringOrNumber(allow_none=True, validate=validate_stretch_range),
        validate=validate.Length(equal=2),
        example="[0,1]",
        missing=None,
        description=(
            "Stretch range [min, max] to use for the blue band as JSON array. "
            "Min and max may be numbers to use as absolute range, or strings "
            "of the format `p<digits>` with an integer between 0 and 100 "
            "to use percentiles of the image instead. "
            "Null values indicate global minimum / maximum."
        ),
    )
    a_range = fields.List(
        StringOrNumber(allow_none=True, validate=validate_stretch_range),
        validate=validate.Length(equal=2),
        example="[0,1]",
        missing=None,
        description=(
            "Stretch range [min, max] to use for the alpha band as JSON array. "
            "Min and max may be numbers to use as absolute range, or strings "
            "of the format `p<digits>` with an integer between 0 and 100 "
            "to use percentiles of the image instead. "
            "Null values indicate global minimum / maximum."
        ),
    )
    tile_size = fields.List(
        fields.Integer(),
        validate=validate.Length(equal=2),
        example="[256,256]",
        description="Pixel dimensions of the returned PNG image as JSON list.",
    )

    @pre_load
    def process_ranges(self, data: Mapping[str, Any], **kwargs: Any) -> Dict[str, Any]:
        data = dict(data.items())
        for var in ("r_range", "g_range", "b_range", "a_range", "tile_size"):
            val = data.get(var)
            if val:
                try:
                    data[var] = json.loads(val)
                except json.decoder.JSONDecodeError as exc:
                    raise ValidationError(
                        f"Could not decode value for {var} as JSON"
                    ) from exc
        return data


@TILE_API.route("/rgba/<int:tile_z>/<int:tile_x>/<int:tile_y>.png", methods=["GET"])
@TILE_API.route(
    "/rgba/<path:keys>/<int:tile_z>/<int:tile_x>/<int:tile_y>.png", methods=["GET"]
)
def get_rgba(tile_z: int, tile_y: int, tile_x: int, keys: str = "") -> Response:
    """Return the requested RGBA tile as a PNG image.
    ---
    get:
        summary: /rgba (tile)
        description: Combine four datasets to RGBA image, and return tile as PNG
        parameters:
            - in: path
              schema: RGBAQuerySchema
            - in: query
              schema: RGBAOptionSchema
        responses:
            200:
                description:
                    PNG image of requested tile
            400:
                description:
                    Invalid query parameters
            404:
                description:
                    No dataset found for given key combination
    """
    logging.debug(f"Received request for RGBA tile: keys={keys}, z={tile_z}, x={tile_x}, y={tile_y}")

    tile_xyz = (tile_x, tile_y, tile_z)
    return _get_rgba_image(keys, tile_xyz=tile_xyz)


def _get_rgba_image(
    keys: str, tile_xyz: Optional[Tuple[int, int, int]] = None
) -> Response:
    from terracotta.handlers.rgba import rgba

    option_schema = RGBAOptionSchema()
    options = option_schema.load(request.args)

    some_keys = [key for key in keys.split("/") if key]

    rgba_values = (
        options.pop("r"),
        options.pop("g"),
        options.pop("b"),
        options.pop("a"),
    )
    stretch_ranges = tuple(
        options.pop(k) for k in ("r_range", "g_range", "b_range", "a_range")
    )

    image = rgba(
        some_keys,
        rgba_values,
        stretch_ranges=stretch_ranges,
        tile_xyz=tile_xyz,
        **options,
    )

    return send_file(image, mimetype="image/png")
