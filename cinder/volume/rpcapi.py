# Copyright 2012, Intel, Inc.
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

"""
Client side of the volume RPC API.
"""

from oslo.config import cfg

from cinder.openstack.common import jsonutils
from cinder.openstack.common import rpc
import cinder.openstack.common.rpc.proxy


CONF = cfg.CONF


class VolumeAPI(cinder.openstack.common.rpc.proxy.RpcProxy):
    '''Client side of the volume rpc API.

    API version history:

        1.0 - Initial version.
        1.1 - Adds clone volume option to create_volume.
        1.2 - Add publish_service_capabilities() method.
        1.3 - Pass all image metadata (not just ID) in copy_volume_to_image.
        1.4 - Add request_spec, filter_properties and
              allow_reschedule arguments to create_volume().
        1.5 - Add accept_transfer.
        1.6 - Add extend_volume.
        1.7 - Adds host_name parameter to attach_volume()
              to allow attaching to host rather than instance.
        1.8 - Add migrate_volume, rename_volume.
        1.9 - Add new_user and new_project to accept_transfer.
        1.10 - Add migrate_volume_completion, remove rename_volume.
        1.11 - Adds mode parameter to attach_volume()
               to support volume read-only attaching.
        1.12 - Adds retype.
    '''

    BASE_RPC_API_VERSION = '1.0'

    def __init__(self, topic=None):
        super(VolumeAPI, self).__init__(
            topic=topic or CONF.volume_topic,
            default_version=self.BASE_RPC_API_VERSION)

    def create_volume(self, ctxt, volume, host,
                      request_spec, filter_properties,
                      allow_reschedule=True,
                      snapshot_id=None, image_id=None,
                      source_volid=None):

        request_spec_p = jsonutils.to_primitive(request_spec)
        self.cast(ctxt,
                  self.make_msg('create_volume',
                                volume_id=volume['id'],
                                request_spec=request_spec_p,
                                filter_properties=filter_properties,
                                allow_reschedule=allow_reschedule,
                                snapshot_id=snapshot_id,
                                image_id=image_id,
                                source_volid=source_volid),
                  topic=rpc.queue_get_for(ctxt,
                                          self.topic,
                                          host),
                  version='1.4')

    def delete_volume(self, ctxt, volume, preserve_sts):
        self.cast(ctxt,
                  self.make_msg('delete_volume',
                                volume_id=volume['id']),
                                'preserve_sts': preserve_sts,
                  topic=rpc.queue_get_for(ctxt, self.topic, volume['host']))

    def create_snapshot(self, ctxt, volume, snapshot):
        self.cast(ctxt, self.make_msg('create_snapshot',
                                      volume_id=volume['id'],
                                      snapshot_id=snapshot['id']),
                  topic=rpc.queue_get_for(ctxt, self.topic, volume['host']))

    def delete_snapshot(self, ctxt, snapshot, host):
        self.cast(ctxt, self.make_msg('delete_snapshot',
                                      snapshot_id=snapshot['id']),
                  topic=rpc.queue_get_for(ctxt, self.topic, host))

    def attach_volume(self, ctxt, volume, instance_uuid, host_name,
                      mountpoint, mode):
        return self.call(ctxt, self.make_msg('attach_volume',
                                             volume_id=volume['id'],
                                             instance_uuid=instance_uuid,
                                             host_name=host_name,
                                             mountpoint=mountpoint,
                                             mode=mode),
                         topic=rpc.queue_get_for(ctxt,
                                                 self.topic,
                                                 volume['host']),
                         version='1.11')

    def detach_volume(self, ctxt, volume):
        return self.call(ctxt, self.make_msg('detach_volume',
                                             volume_id=volume['id']),
                         topic=rpc.queue_get_for(ctxt,
                                                 self.topic,
                                                 volume['host']))

    def copy_volume_to_image(self, ctxt, volume, image_meta):
        self.cast(ctxt, self.make_msg('copy_volume_to_image',
                                      volume_id=volume['id'],
                                      image_meta=image_meta),
                  topic=rpc.queue_get_for(ctxt,
                                          self.topic,
                                          volume['host']),
                  version='1.3')

    def initialize_connection(self, ctxt, volume, connector):
        return self.call(ctxt, self.make_msg('initialize_connection',
                                             volume_id=volume['id'],
                                             connector=connector),
                         topic=rpc.queue_get_for(ctxt,
                                                 self.topic,
                                                 volume['host']))

    def terminate_connection(self, ctxt, volume, connector, force=False):
        return self.call(ctxt, self.make_msg('terminate_connection',
                                             volume_id=volume['id'],
                                             connector=connector,
                                             force=force),
                         topic=rpc.queue_get_for(ctxt,
                                                 self.topic,
                                                 volume['host']))

    def publish_service_capabilities(self, ctxt):
        self.fanout_cast(ctxt, self.make_msg('publish_service_capabilities'),
                         version='1.2')

    def accept_transfer(self, ctxt, volume, new_user, new_project):
        self.cast(ctxt,
                  self.make_msg('accept_transfer',
                                volume_id=volume['id'],
                                new_user=new_user,
                                new_project=new_project),
                  topic=rpc.queue_get_for(ctxt, self.topic, volume['host']),
                  version='1.9')

    def extend_volume(self, ctxt, volume, new_size):
        self.cast(ctxt,
                  self.make_msg('extend_volume',
                                volume_id=volume['id'],
                                new_size=new_size),
                  topic=rpc.queue_get_for(ctxt, self.topic, volume['host']),
                  version='1.6')

    def migrate_volume(self, ctxt, volume, dest_host, force_host_copy):
        host_p = {'host': dest_host.host,
                  'capabilities': dest_host.capabilities}
        self.cast(ctxt,
                  self.make_msg('migrate_volume',
                                volume_id=volume['id'],
                                host=host_p,
                                force_host_copy=force_host_copy),
                  topic=rpc.queue_get_for(ctxt, self.topic, volume['host']),
                  version='1.8')

    def migrate_volume_completion(self, ctxt, volume, new_volume, error):
        return self.call(ctxt,
                         self.make_msg('migrate_volume_completion',
                                       volume_id=volume['id'],
                                       new_volume_id=new_volume['id'],
                                       error=error),
                         topic=rpc.queue_get_for(ctxt, self.topic,
                                                 volume['host']),
                         version='1.10')

    def retype(self, ctxt, volume, new_type_id, dest_host,
               migration_policy='never', reservations=None):
        host_p = {'host': dest_host.host,
                  'capabilities': dest_host.capabilities}
        self.cast(ctxt,
                  self.make_msg('retype',
                                volume_id=volume['id'],
                                new_type_id=new_type_id,
                                host=host_p,
                                migration_policy=migration_policy,
                                reservations=reservations),
                  topic=rpc.queue_get_for(ctxt, self.topic, volume['host']),
                  version='1.12')
