"""scripts/optimize_rasters.py

Convert some raster files to cloud-optimized GeoTiff for use with Terracotta.
"""

from typing import Sequence
import os
import math
import itertools
import contextlib
import tempfile
from pathlib import Path

import click
from rasterio.enums import Resampling

from terracotta.scripts.click_utils import GlobbityGlob, PathlibPath

IN_MEMORY_THRESHOLD = 10980 * 10980

CACHEMAX = 1024 * 1024 * 512  # 512 MB

GDAL_CONFIG = {
    'GDAL_NUM_THREADS': 'ALL_CPUS',
    'GDAL_TIFF_INTERNAL_MASK': True,
    'GDAL_TIFF_OVR_BLOCKSIZE': 256,
    'GDAL_CACHEMAX': CACHEMAX,
    'GDAL_SWATH_SIZE': 2 * CACHEMAX
}

COG_PROFILE = {
    'count': 1,
    'driver': 'GTiff',
    'interleave': 'pixel',
    'tiled': True,
    'blockxsize': 256,
    'blockysize': 256,
    'compress': 'DEFLATE',
    'photometric': 'MINISBLACK',
    'BIGTIFF': 'IF_SAFER'
}

RESAMPLING_METHODS = {
    'average': Resampling.average,
    'nearest': Resampling.nearest,
    'bilinear': Resampling.bilinear,
    'cubic': Resampling.cubic
}


@click.command('optimize-rasters',
               short_help='Optimize a collection of raster files for use with Terracotta.')
@click.argument('raster-files', nargs=-1, type=GlobbityGlob(), required=True)
@click.option('-o', '--output-folder', type=PathlibPath(file_okay=False), required=True,
              help='Output folder for cloud-optimized rasters. Subdirectories will be flattened.')
@click.option('--overwrite', is_flag=True, default=False,
              help='Force overwrite of existing files')
@click.option('--resampling-method', type=click.Choice(RESAMPLING_METHODS.keys()),
              default='average', help='Resampling method for overviews', show_default=True)
@click.option('--in-memory/--no-in-memory', default=None,
              help='Force processing raster in memory / not in memory [default: process in memory '
                   f'if smaller than {IN_MEMORY_THRESHOLD // 1e6:.0f} million pixels]')
def optimize_rasters(raster_files: Sequence[Sequence[Path]],
                     output_folder: Path,
                     overwrite: bool = False,
                     resampling_method: str = 'average',
                     in_memory: bool = None) -> None:
    """Optimize a collection of raster files for use with Terracotta.

    First argument is a list of input files or glob patterns.

    Example:

        terracotta optimize-rasters rasters/*.tif -o cloud-optimized/

    Note that all rasters may only contain a single band.
    """
    import rasterio
    from rasterio.io import MemoryFile
    from rasterio.shutil import copy

    raster_files_flat = sorted(set(itertools.chain.from_iterable(raster_files)))

    if not raster_files_flat:
        click.echo('No files given')
        return

    for f in raster_files_flat:
        if not f.is_file():
            raise click.Abort(f'Input raster {f!s} is not a file')

    output_folder.mkdir(exist_ok=True)

    pbar_args = dict(
        label='Optimizing raster files',
        show_eta=False,
        item_show_func=lambda s: s.name if s else ''
    )

    click.echo('')
    with click.progressbar(raster_files_flat, **pbar_args) as pbar:  # type: ignore
        for input_file in pbar:
            output_file = output_folder / input_file.with_suffix('.tif').name

            if not overwrite and output_file.is_file():
                click.echo('')
                click.echo(f'Output file {output_file!s} exists (use --overwrite to ignore)')
                raise click.Abort()

            with contextlib.ExitStack() as es:
                es.enter_context(rasterio.Env(**GDAL_CONFIG))
                src = es.enter_context(rasterio.open(str(input_file)))

                profile = src.profile.copy()
                profile.update(COG_PROFILE)

                if in_memory is None:
                    in_memory = src.width * src.height < IN_MEMORY_THRESHOLD

                if in_memory:
                    memfile = es.enter_context(MemoryFile())
                    dst = es.enter_context(memfile.open(**profile))
                else:
                    tempdir = es.enter_context(tempfile.TemporaryDirectory())
                    tempraster = os.path.join(tempdir, 'tc-raster.tif')
                    dst = es.enter_context(rasterio.open(tempraster, 'w', **profile))

                # iterate over blocks
                windows = list(dst.block_windows(1))

                for _, w in windows:
                    block_data = src.read(window=w, indexes=[1])
                    dst.write(block_data, window=w)
                    block_mask = src.dataset_mask(window=w)
                    dst.write_mask(block_mask, window=w)

                # add overviews
                max_overview_level = math.ceil(math.log2(max(
                    dst.height // profile['blockysize'],
                    dst.width // profile['blockxsize']
                )))

                overviews = [2 ** j for j in range(1, max_overview_level + 1)]
                rs_method = RESAMPLING_METHODS[resampling_method]
                dst.build_overviews(overviews, rs_method)
                dst.update_tags(ns='tc_overview', resampling=rs_method.value)

                # copy to destination (this is necessary to produce a consistent file)
                copy(dst, str(output_file), copy_src_overviews=True, **COG_PROFILE)
