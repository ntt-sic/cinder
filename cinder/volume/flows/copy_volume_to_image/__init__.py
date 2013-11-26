# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (c) 2013 NTT.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import taskflow.engines
from taskflow.patterns import linear_flow
from taskflow import task

from cinder import exception
from cinder.image import glance
from cinder.openstack.common import excutils
from cinder.openstack.common import log as logging
from cinder.openstack.common.gettextutils import _
from cinder.volume.flows import base

LOG = logging.getLogger(__name__)

ACTION = 'volume:copy_volume_to_image'


class ValidateVolumeAvailabilityTask(base.CinderTask):
    """
    Validate volume against a set of conditions and
    determine whether it should be allowed for copying.
    """

    def __init__(self, **kwargs):
        provides = 'volume_status'
        requires = ('volume', 'force')
        super(ValidateVolumeAvailabilityTask, self).\
            __init__(addons=[ACTION], requires=requires,
                     provides=provides, **kwargs)

    def execute(self, volume, force):
        if volume['status'] not in ['available', 'in-use']:
            msg = _('Volume status must be available/in-use.')
            raise exception.InvalidVolume(reason=msg)
        if not force and 'in-use' == volume['status']:
            msg = _('Volume status is in-use.')
            raise exception.InvalidVolume(reason=msg)

        return volume['status']


class CreateImageServiceTask(base.CinderTask):
    """
    Creates an empty image as per image_metadata with no image data
    """

    def __init__(self, image_service, **kwargs):
        self.image_service = image_service
        provides = 'image_metadata'
        super(CreateImageServiceTask, self). \
            __init__(addons=[ACTION], provides=provides, **kwargs)

    def execute(self, context, metadata):
        image_metadata = self.image_service.create(context, metadata)
        return image_metadata

    def revert(self, result, context, **kwargs):
        # We never produced a result and therefore can't destroy anything.
        if not result:
            return

        image_id = result['id']
        self.image_service.delete(context, image_id)


class UpdateVolumeStatusTask(base.CinderTask):
    """
    Update volume status to 'uploading'
    """

    def __init__(self, db, **kwargs):
        self.db = db
        provides = 'volume'
        requires = ('context', 'volume_id', 'volume_status')
        super(UpdateVolumeStatusTask, self). \
            __init__(addons=[ACTION], requires=requires,
                     provides=provides, **kwargs)

    def execute(self, context, volume_id, volume_status):
        self.db.volume_update(context, volume_id, {'status': 'uploading'})
        volume = self.db.volume_get(context, volume_id)
        return volume

    def revert(self, context, volume_id, volume_status, **kwargs):
        self.db.volume_update(context, volume_id, {'status': volume_status})


class CopyVolumeToImageCastTask(base.CinderTask):
    """
    Performs a volume copy to image cast to the the volume manager.
    This will signal a transition of the api workflow to another child
    and/or related workflow on another component.

    Reversion strategy: N/A
    """

    def __init__(self, volume_rpcapi, **kwargs):
        self.volume_rpcapi = volume_rpcapi
        requires = ('context', 'volume', 'image_metadata')
        super(CopyVolumeToImageCastTask, self). \
            __init__(addons=[ACTION], requires=requires, **kwargs)

    def execute(self, context, volume, image_metadata):
        self.volume_rpcapi.copy_volume_to_image(context,
                                                volume,
                                                image_metadata)


class OnFailureChangeStatusTask(base.CinderTask):
    """
    Helper task that sets a volume status to "available" or "in-use" depending
    on whether the volume is attached to the instance or not.
    """

    def __init__(self, db, **kwargs):
        self.db = db
        requires = ('context', 'volume_id')
        super(OnFailureChangeStatusTask, self). \
            __init__(addons=[ACTION], requires=requires, **kwargs)

    def execute(self, context, volume_id):
        return {'volume_id': volume_id, 'context': context}

    def revert(self, context, volume_id, *args, **kwargs):
        #Change volume status
        volume = self.db.volume_get(context, volume_id)
        if (volume['instance_uuid'] is None and
                volume['attached_host'] is None):
            self.db.volume_update(context, volume_id,
                                  {'status': 'available'})
        else:
            self.db.volume_update(context, volume_id,
                                  {'status': 'in-use'})


class ExtractVolumeSpecTask(base.CinderTask):
    """
    Extract volume specs from db.
    """

    def __init__(self, db, **kwargs):
        self.db = db
        requires = ('context', 'volume_id')
        provides = 'volume'
        super(ExtractVolumeSpecTask, self). \
            __init__(addons=[ACTION], requires=requires,
                     provides=provides, **kwargs)

    def execute(self, context, volume_id):
        volume = self.db.volume_get(context, volume_id)
        return volume


class CopyVolumeToImageTask(base.CinderTask):
    """
    Performs ensure export and copy volume to image call to driver.
    """

    def __init__(self, driver, **kwargs):
        self.driver = driver
        requires = ('context', 'volume', 'image_metadata')
        super(CopyVolumeToImageTask, self). \
            __init__(addons=[ACTION], requires=requires, **kwargs)

    def execute(self, context, volume, image_metadata):
        "Uploads the specified volume to Glance."
        self.driver.ensure_export(context.elevated(), volume)
        image_service, image_id = \
            glance.get_remote_image_service(context, image_metadata['id'])
        self.driver.copy_volume_to_image(context, volume,
                                         image_service,
                                         image_metadata)
        LOG.debug(_("Uploaded volume %(volume_id)s to "
                    "image (%(image_id)s) successfully"),
                  {'volume_id': volume['id'],
                   'image_id': image_metadata['id']})


class CopyVolumeToImageOnFinishTask(base.CinderTask):
    """
    Update volume status after copying volume to image
    """

    def __init__(self, db, **kwargs):
        self.db = db
        requires = ('context', 'volume')
        super(CopyVolumeToImageOnFinishTask, self). \
            __init__(addons=[ACTION], requires=requires, **kwargs)

    def execute(self, context, volume):
        volume_id = volume['id']
        if (volume['instance_uuid'] is None and
                volume['attached_host'] is None):
            self.db.volume_update(context, volume_id,
                                  {'status': 'available'})
        else:
            self.db.volume_update(context, volume_id,
                                  {'status': 'in-use'})


def get_api_flow(volume_rpcapi, image_service, db, create_what):
    """Constructs and returns the api entrypoint flow.

    This flow will do the following:

    1. Inject keys & values for dependent tasks.
    2. Validates volume status.
    3. Create empty image.
    4. Update volume status.
    5. Casts to volume manager for further processing.
    """

    flow_name = ACTION.replace(":", "_") + "_api"
    api_flow = linear_flow.Flow(flow_name)

    api_flow.add(base.InjectTask(create_what, addons=[ACTION]))
    api_flow.add(ValidateVolumeAvailabilityTask())
    api_flow.add(CreateImageServiceTask(image_service))
    api_flow.add(UpdateVolumeStatusTask(db))
    api_flow.add(CopyVolumeToImageCastTask(volume_rpcapi))

    engine = taskflow.engines.load(api_flow, store=create_what)

    return engine


def get_manager_flow(driver, db, create_what):
    """Constructs and returns the manager entrypoint flow.

    This flow will do the following:

    1. Inject keys & values for dependent tasks.
    2. On failure change volume status.
    3. Get volume specs from db.
    4. Get remote Image service from metadata.
    5. Copy volume to image.
    6. Update volume status depending on whether the volume
       is attached to the instance or not.
    """

    flow_name = ACTION.replace(":", "_") + "_manager"
    api_flow = linear_flow.Flow(flow_name)

    api_flow.add(base.InjectTask(create_what, addons=[ACTION]))
    api_flow.add(OnFailureChangeStatusTask(db))
    api_flow.add(ExtractVolumeSpecTask(db))
    api_flow.add(CopyVolumeToImageTask(driver))
    api_flow.add(CopyVolumeToImageOnFinishTask(db))

    engine = taskflow.engines.load(api_flow, store=create_what)

    return engine
