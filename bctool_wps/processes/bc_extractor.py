from pywps import Process, LiteralInput, LiteralOutput, UOM, ComplexInput, ComplexOutput, Format, FORMATS
from pywps.app.Common import Metadata

import logging
LOGGER = logging.getLogger("PYWPS")

import os
import subprocess

# https://github.com/bird-house/flyingpigeon/blob/master/flyingpigeon/processes/wps_subset_countries.py
from pywps.inout.outputs import MetaFile, MetaLink4


class BCExtractor(Process):
    """BC WRF Extractor"""
    def __init__(self):
        inputs = [
            LiteralInput('start_datetime',
                         'Initial date to process as YYYY-MM-DD_HH:MM:SS',
                         min_occurs=1,
                         max_occurs=1,
                         data_type='string'),
            LiteralInput('end_datetime',
                         'End date to process as YYYY-MM-DD_HH:MM:SS',
                         min_occurs=1,
                         max_occurs=1,
                         data_type='string'),
            ComplexInput('bc_table', 'BC table',
                         abstract='BCtable',
                         min_occurs=1,
                         max_occurs=1,
                         supported_formats=[
                             Format('text/plain'),
                         ]),
        ]

        outputs = [
             ComplexOutput('metalink',
                           'Metalink file with links to all BC outputs.',
                           as_reference=True,
                           supported_formats=[FORMATS.META4]),
             LiteralOutput('stdout', 'stdout', data_type='string'),
             LiteralOutput('stderr', 'stderr', data_type='string')]

        super(BCExtractor, self).__init__(
            self._handler,
            identifier='bcextractor',
            title='BC Extractor',
            abstract='Boundary Condition extractor for WRF model',
            keywords=['WRF', 'boundary'],
            metadata=[
                Metadata('PyWPS', 'https://pywps.org/'),
                Metadata('Birdhouse', 'http://bird-house.github.io/'),
                Metadata('PyWPS Demo', 'https://pywps-demo.readthedocs.io/en/latest/'),
                Metadata('Emu: PyWPS examples', 'https://emu.readthedocs.io/en/latest/'),
            ],
            version='1.5',
            inputs=inputs,
            outputs=outputs,
            store_supported=True,
            status_supported=True
        )

    def _handler(self, request, response):
        LOGGER.info("Extract boundary conditions")

        bc_table = request.inputs['bc_table'][0].file
        start_datetime = request.inputs['start_datetime'][0].data
        end_datetime = request.inputs['end_datetime'][0].data
        output_directory = self.workdir

        command = ["bctool/preprocessor.ESGF", start_datetime, end_datetime, "/oceano/gmeteo/WORK/ASNA/DATA/CanESM2", bc_table, output_directory]
        bc = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        outlog = bc.stdout.decode("utf-8")
        errlog = bc.stderr.decode("utf-8")

        
        try:
            ml = MetaLink4('bc', workdir=output_directory)
            for f in os.listdir(output_directory):
                mf = MetaFile(os.path.basename(f), fmt=FORMATS.META4)
                mf.file = os.path.join(output_directory, f)
                ml.append(mf)
        except Exception as ex:
            msg='BC failed: {}'.format(str(ex))
            LOGGER.exception(msg)
            raise Exception(msg)
        
        response.outputs['metalink'].data = ml.xml
        response.outputs['stdout'].data = outlog
        response.outputs['stderr'].data = errlog
        response.update_status("Completed", 100)
        return response
