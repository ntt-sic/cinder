# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright(c)2012-2013 NTT corp. All Rights Reserved.
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
Drivers for EMC-stored volumes.

This file is delivered from san.py.
"""

import copy
from datetime import datetime
import os
import paramiko
import time

from cinder import context
from cinder import exception
from cinder import flags
from cinder.image import glance
from cinder.openstack.common import log as logging
from cinder.openstack.common import cfg
from cinder.openstack.common import lockutils
from cinder import utils
from cinder.volume.driver import ISCSIDriver
from cinder.volume import utils as volume_utils
from cinder.openstack.common.gettextutils import _

LOG = logging.getLogger(__name__)

emc_nscli_opts = [
    cfg.StrOpt('emc_nscli_cmd',
               default='/opt/Navisphere/bin/naviseccli',
               help='command path of NaviSphere CLI'),
    cfg.StrOpt('emc_nscli_ip',
               default='',
               help='IP address of SAN controller(a)'),
    cfg.StrOpt('emc_nscli_ip2',
               default='',
               help='IP address of SAN controller(b)'),
    cfg.StrOpt('emc_nscli_service_ip',
               default='',
               help='IP address of iSCSI Target'),
    cfg.StrOpt('emc_nscli_service_ip2',
               default='',
               help='IP address of additional iSCSI Target(2)'),
    cfg.StrOpt('emc_nscli_service_ip3',
               default='',
               help='IP address of additional iSCSI Target(3)'),
    cfg.StrOpt('emc_nscli_service_ip4',
               default='',
               help='IP address of additional iSCSI Target(4)'),
    cfg.IntOpt('emc_nscli_service_port',
               default=3260,
               help='TCP port of iSCSI Target'),
    cfg.IntOpt('emc_nscli_service_port2',
               default=3260,
               help='TCP port of additional iSCSI Target(2)'),
    cfg.IntOpt('emc_nscli_service_port3',
               default=3260,
               help='TCP port of additional iSCSI Target(3)'),
    cfg.IntOpt('emc_nscli_service_port4',
               default=3260,
               help='TCP port of additional iSCSI Target(4)'),
    cfg.IntOpt('emc_nscli_service_portal_group_tag',
               default=1,
               help='iSCSI Portal Group Tag'),
    cfg.IntOpt('emc_nscli_service_portal_group_tag2',
               default=1,
               help='iSCSI Portal Group Tag(2)'),
    cfg.IntOpt('emc_nscli_service_portal_group_tag3',
               default=1,
               help='iSCSI Portal Group Tag(3)'),
    cfg.IntOpt('emc_nscli_service_portal_group_tag4',
               default=1,
               help='iSCSI Portal Group Tag(4)'),
    cfg.StrOpt('emc_nscli_target_iqn',
               default='',
               help='IQN of iSCSI Target'),
    cfg.StrOpt('emc_nscli_target_iqn2',
               default='',
               help='IQN of additional iSCSI Target(2)'),
    cfg.StrOpt('emc_nscli_target_iqn3',
               default='',
               help='IQN of additional iSCSI Target(3)'),
    cfg.StrOpt('emc_nscli_target_iqn4',
               default='',
               help='IQN of additional iSCSI Target(4)'),
    cfg.StrOpt('emc_nscli_sync_rate',
               default='high',
               help='Clone synchronize rate'),
    cfg.IntOpt('emc_nscli_port',
               default=443,
               help='port to use with NaviSphere CLI(a)'),
    cfg.IntOpt('emc_nscli_port2',
               default=443,
               help='port to use with NaviSphere CLI(b)'),
    cfg.StrOpt('emc_nscli_login',
               default='',
               help='Username for SAN controller(a)'),
    cfg.StrOpt('emc_nscli_login2',
               default='',
               help='Username for SAN controller(b)'),
    cfg.StrOpt('emc_nscli_password',
               default='',
               help='Password for SAN controller(a)'),
    cfg.StrOpt('emc_nscli_password2',
               default='',
               help='Password for SAN controller(b)'),
    cfg.IntOpt('emc_nscli_scope',
               default=0,
               help='scope of user account (0:global, 1:local, 2:LDAP)'),
    cfg.IntOpt('emc_nscli_pool_id',
               default=0,
               help='storage pool id used by nova-volume'),
    cfg.IntOpt('emc_nscli_min_alu',
               default=0,
               help='Minimum iscsi target number'),
    cfg.IntOpt('emc_nscli_max_alu',
               default=4095,
               help='Max iscsi target number'),
    cfg.IntOpt('emc_nscli_min_hlu',
               default=1,
               help='Minimum host lun number'),
    cfg.IntOpt('emc_nscli_max_hlu',
               default=256,
               help='Max host lun number'),
    cfg.StrOpt('emc_nscli_snapshot_name_format',
               default='Snap_%s',
               help='format string of snapshot'),
    cfg.StrOpt('emc_nscli_storage_processor',
               default='a',
               help='owner processor of the lun'),
    cfg.IntOpt('emc_nscli_cmd_count',
               default=1000,
               help='Retry count for waiting clone-volume'),
    cfg.IntOpt('emc_nscli_udev_timeout',
               default=120,
               help='Seconds to wait udev events finished'),
    cfg.IntOpt('emc_nscli_internal_process_sec',
               default=60,
               help='Time until finish an ongoing storage ineternal process'),
    cfg.IntOpt('emc_nscli_max_snapshot_count',
               default=7,
               help='Max snapshot count'),
    cfg.StrOpt('emc_nscli_snapshot_session_name_format',
               default='Session_%s',
               help='format string of session'),
    cfg.IntOpt('emc_nscli_max_storage_group_count',
               default=250,
               help='Max storage group count'),
    cfg.IntOpt('emc_nscli_ssh_port',
               default=22,
               help='port to use with SSH to nova-compute'),
    cfg.StrOpt('emc_nscli_ssh_login',
               default='cinder',
               help='Username for connect SSH to nova-compute'),
    cfg.StrOpt('emc_nscli_ssh_private_key',
               default='',
               help='Private key for connect SSH to nova-compute'),
    cfg.IntOpt('multipath_remove_retries',
               default=10,
               help='Retry count of removing multipath map'),
    cfg.IntOpt('multipath_list_retries',
               default=5,
               help='Retry count of listing multipath map'),
    cfg.IntOpt('multipath_cli_exec_timeout',
               default=30,
               help='more than 0, multipath-cli timeout function is' +\
                    ' effective'),
    cfg.IntOpt('emc_nscli_exec_timeout',
               default=30,
               help='more than 0, the ncsli time-out function is effective'),
    cfg.IntOpt('emc_nscli_clone_sync_timeout',
               default=1800,
               help='Clone synchronization time-out'),
    cfg.IntOpt('emc_nscli_clone_sleep_time',
               default=120,
               help='The interval between clone synchronization checks'),
    cfg.IntOpt('emc_alu_reuse_interval',
               default=180,
               help='time for waiting to re-use alu after deleting volume'),
    ]

FLAGS = flags.FLAGS
FLAGS.register_opts(emc_nscli_opts)

EXEC_CHECK_CMD_TABLE = {
            #check create lun, Insert 'alu' as the second.
            'lun_create':
                ['getlun', '-name'],
            #check storage group add hlu, Insert 'gname' as the fourth.
            'sg_add_hlu':
                ['storagegroup', '-list', '-gname'],
            #check storage group remove hlu, Insert 'gname' as the fourth.
            'sg_remove_hlu':
                ['storagegroup', '-list', '-gname'],
            #check destroy lun, Insert 'alu' as the second.
            'lun_destroy':
                ['getlun', '-name'],
            #check storage group disconnect host, Insert 'gname' as the fourth.
            'sg_disconnect_host':
                ['storagegroup', '-list', '-gname'],
            #check storage group destroy, Insert 'gname' as the fourth.
            'sg_destroy':
                ['storagegroup', '-list', '-gname'],
            #check create snapshot, Insert 'snapshotname' as the fifth.
            'snapshot_create':
                ['snapview', '-listsnapshots', '-name', '-snapshotname'],
            #check start session, Insert 'sessionname' as the fifth.
            'start_session':
                ['snapview', '-listsessions', '-snapshotsname', '-name'],
            #check storage group add snapshot, Insert 'gname' as the fourth.
            'sg_add_snapshot':
                ['storagegroup', '-list', '-gname'],
            #check activate snapshot, Insert 'snapshotname' as the fifth.
            'snapshot_activate':
                ['snapview', '-listsnapshots', '-name', '-snapshotname'],
            #check deactivate snapshot, Insert 'snapshotname' as the fifth.
            'snapshot_deactivate':
                ['snapview', '-listsnapshots', '-name', '-snapshotname'],
            #check stop session, Insert 'sessionname' as the fifth.
            'stop_session':
                ['snapview', '-listsessions', '-snapshotsname', '-name'],
            #check remove snapshot, Insert 'snapshotname' as the fifth.
            'snapshot_remove':
                ['snapview', '-listsnapshots', '-name', '-snapshotname'],
            #check storage group remove snapshot, Insert 'gname' as the fourth.
            'sg_remove_snapshot':
                ['storagegroup', '-list', '-gname'],
            #check list clone group, Insert 'Name' as the fourth.
            'clone_list_clonegroup':
                ['snapview', '-listclonegroup', '-Name'],
            #check list clone, Insert 'Name' as the fourth.
            'clone_list_clone':
                ['snapview', '-listclone', '-Name'],
            #check create clone group, Insert 'name' as the fourth.
            'clone_create_clonegroup':
                ['snapview', '-listclonegroup', '-Name'],
            #check add clone to clone group, Insert 'name' as the fourth.
            'clone_add_clone':
                ['snapview', '-listclone', '-Name', '-CloneLuns'],
            #check fracture clone, Insert 'name' as the fourth.
            'clone_fracture_clone':
                ['snapview', '-listclone', '-Name', '-cloneid', '-CloneState'],
            #check remove clone, Insert 'name' as the fourth.
            'clone_remove_clone':
                ['snapview', '-listclone', '-Name', '-cloneid'],
            #check destroy clone group, Insert 'name' as the fourth.
            'clone_destroy_clonegroup':
                ['snapview', '-listclonegroup', '-Name'],
            }


class EMCVNXISCSIDriver(ISCSIDriver):
    """Executes commands relating to EMC VNX 5300 ISCSI volumes."""

    def __init__(self, execute=utils.execute, *args, **kwargs):
        super(EMCVNXISCSIDriver, self).__init__(*args, **kwargs)
        self.db = None
        self._execute = execute
        self.host = FLAGS.host
        self.admin_context = context.get_admin_context()
        self.min_hlu = FLAGS.emc_nscli_min_hlu
        self.max_hlu = FLAGS.emc_nscli_max_hlu
        self.min_alu = FLAGS.emc_nscli_min_alu
        self.max_alu = FLAGS.emc_nscli_max_alu
        self.target_portal = []
        self.target_portal_group_tag = []
        self.target_iqn = []
        target_iqns = [FLAGS.emc_nscli_target_iqn,
                       FLAGS.emc_nscli_target_iqn2,
                       FLAGS.emc_nscli_target_iqn3,
                       FLAGS.emc_nscli_target_iqn4]
        service_ips = [FLAGS.emc_nscli_service_ip,
                       FLAGS.emc_nscli_service_ip2,
                       FLAGS.emc_nscli_service_ip3,
                       FLAGS.emc_nscli_service_ip4]
        service_port = [FLAGS.emc_nscli_service_port,
                        FLAGS.emc_nscli_service_port2,
                        FLAGS.emc_nscli_service_port3,
                        FLAGS.emc_nscli_service_port4]
        target_portal_group_tag = [FLAGS.emc_nscli_service_portal_group_tag,
                                   FLAGS.emc_nscli_service_portal_group_tag2,
                                   FLAGS.emc_nscli_service_portal_group_tag3,
                                   FLAGS.emc_nscli_service_portal_group_tag4]

        for iqn, ip, port, pgt in zip(target_iqns, service_ips, service_port,
                                      target_portal_group_tag):
            if iqn and ip and port and pgt:
                self.target_portal.append("%s:%s" % (ip, port))
                self.target_portal_group_tag.append(pgt)
                self.target_iqn.append(iqn)
        self.storage_processor = FLAGS.emc_nscli_storage_processor
        self.emc_nscli_ip = {
            'a': FLAGS.emc_nscli_ip,
            'b': FLAGS.emc_nscli_ip2,
        }

    def set_execute(self, execute):
        self._execute = execute

    def set_parameters(self, **kwargs):
        for key, value in kwargs.iteritems():
            if self.__dict__.get(key):
                self.__dict__[key] = value

    def _change_nscli_ctrl_info(self, init=False):
        """SP which is being used is changed."""
        if self.storage_processor == 'a':
            if FLAGS.emc_nscli_ip2 == '':
                LOG.info(_("emc_nscli_ip2 isn't set"))
                return
            self.storage_processor = 'b'
        else:
            if FLAGS.emc_nscli_ip == '':
                LOG.info(_("emc_nscli_ip isn't set"))
                return
            self.storage_processor = 'a'
        self.db.emc_set_current_sp(self.admin_context, self.host,
                                    self.storage_processor)
        if init:
            LOG.info(_("Storage processor initialized: %s, ip: %s") %
                    (self.storage_processor,
                    self.emc_nscli_ip[self.storage_processor]))
        else:
            LOG.info(_("Storage processor changed: %s, ip: %s") %
                    (self.storage_processor,
                    self.emc_nscli_ip[self.storage_processor]))

    def _get_iscsi_properties(self, volume):
        properties = {}
        # provider_location is always set.
        location = volume['provider_location']
        properties['target_discovered'] = True
        results = location.split(" ")
        properties['target_portal'] = self.target_portal
        properties['target_portal_group_tag'] = self.target_portal_group_tag
        properties['target_iqn'] = self.target_iqn
        properties['target_lun'] = int(results[2])
        properties['volume_id'] = volume['id']
        return properties

    @staticmethod
    def _build_snapshot_name(snapshot):
        return "%s" % (FLAGS.emc_nscli_snapshot_name_format % snapshot['id'])

    @staticmethod
    def _build_snapshot_session_name(snapshot):
        return "%s" % \
            (FLAGS.emc_nscli_snapshot_session_name_format % snapshot['id'])

    def _get_existing_host_devices(self, hlu):
        """Get existing host device paths"""
        existing_host_devices = []
        for target_num in range(len(self.target_iqn)):
            host_device = ("/dev/disk/by-path/ip-%s-iscsi-%s-lun-%s" %
                            (self.target_portal[target_num],
                             self.target_iqn[target_num],
                             hlu))
            if os.path.exists(host_device):
                existing_host_devices.append(host_device)

        return existing_host_devices

    def _change_multipath(self, path):
        """Change to the multipath from host device path"""
        # Identify the device file name
        link_path = []
        for i_path in path:
            link_path.append(os.path.realpath(i_path))

        # Get multipath name
        _, multipath_name = self._get_multipath_name_and_status(link_path)

        # Get multipath full path
        multipath_full_path = '/dev/mapper/' + multipath_name

        return multipath_full_path

    def _get_multipath_name_and_status(self, link_path):
        """Identify the multipath name and return path status"""
        device_file_name = []
        path_status = {}
        for i_path in link_path:
            device_file_name.append(i_path.rsplit('/', 1)[1])
            path_status[device_file_name[-1]] = False
        multipath_name = None

        for i in range(0, FLAGS.multipath_list_retries):
            time.sleep(i ** 2)
            self._execute('udevadm', 'settle',
                          '--timeout=%s' % FLAGS.emc_nscli_udev_timeout,
                          run_as_root=True,
                          check_exit_code=[0, 1])
            # get multipath maps
            (out, err) = self._execute('multipath', '-l',
                                        run_as_root=True)
            LOG.debug("multipath -l : stdout=%s stderr=%s" %
                      (out, err))

            # Check if the list of multipath exists
            if out == "":
                raise exception.CinderException(
                    _("list of multipath doesn't exist"))

            lines = out.splitlines()
            for device_file_name_index, line in enumerate(lines):
                for i_device in device_file_name:
                    if ' ' + i_device + ' ' in line:
                        # Update status if the path is active
                        path_status[i_device] = 'active' in line
                        if multipath_name is None:
                            for line in lines[device_file_name_index::-1]:
                                if 'dm-' in line:
                                    multipath_name = line.split(' ')[0]
                                    break
                        break
            if not multipath_name is None:
                break

            # Kick multipathd again.
            for i_path in link_path:
                self._execute('multipath', i_path, run_as_root=True,
                              check_exit_code=False)

        else:
            raise exception.CinderException(
                _("multipath name( device file name=%s ) not found") %
                (device_file_name))

        return path_status, multipath_name

    def _has_snapshot(self, volume):
        volume_id = volume['id']
        ctxt = self.admin_context
        for _snapshot in self.db.snapshot_get_all(ctxt):
            if volume_id == _snapshot['volume_id']:
                return True
        return False

    def _has_cloned_volume(self, snapshot):
        snapshot_id = snapshot['id']
        ctxt = self.admin_context
        for _volume in self.db.volume_get_all(ctxt):
            if snapshot_id == _volume['snapshot_id']:
                return True
        return False

    def _ensure_emc_hlus(self, ctxt, host):
        """Ensure that host luns have been created in datastore."""
        host_luns = self.db.emc_hlu_count_by_host(ctxt, self.host, host,
                                                  self.min_hlu, self.max_hlu)
        if host_luns >= self.max_hlu - self.min_hlu + 1:
            return
        for hlu_num in xrange(self.min_hlu, self.max_hlu + 1):
            target = {'volume_host': self.host,
                      'host': host,
                      'hlu': hlu_num}
            self.db.emc_hlu_create_safe(ctxt, target)

    def _run_iscsiadm_all(self, iscsi_command, **kwargs):
        """Execute command of iscsiadm to all target"""
        target_total_num = len(self.target_iqn)
        failed_count = 0
        last_exc = None
        for target_num in range(target_total_num):
            try:
                self._run_iscsiadm_one(target_num,
                                       iscsi_command, **kwargs)
            except exception.ProcessExecutionError as exc:
                failed_count += 1
                last_exc = exc

        if target_total_num == failed_count:
            # failed to all target
            raise last_exc

    def _run_iscsiadm_one(self, target_num, iscsi_command, **kwargs):
        check_exit_code = kwargs.pop('check_exit_code', 0)
        if '--op' in iscsi_command and 'new' in iscsi_command\
        and iscsi_command.index('new') - iscsi_command.index('--op') == 1:
            (out, err) = self._execute('iscsiadm', '-m', 'node', '-T',
                                       self.target_iqn[target_num],
                                       '-p', "%s,%s" %
                                       (self.target_portal[target_num],
                                        self.target_portal_group_tag[target_num]),
                                       *iscsi_command, run_as_root=True,
                                       check_exit_code=check_exit_code)
        else:
            (out, err) = self._execute('iscsiadm', '-m', 'node', '-T',
                                       self.target_iqn[target_num],
                                       '-p', self.target_portal[target_num],
                                       *iscsi_command, run_as_root=True,
                                       check_exit_code=check_exit_code)
        LOG.debug("iscsiadm %s: stdout=%s stderr=%s" %
                  (iscsi_command, out, err))
        return (out, err)

    def _iscsiadm_update(self, property_key, property_value,
                         **kwargs):
        iscsi_command = ('--op', 'update', '-n', property_key,
                         '-v', property_value)
        return self._run_iscsiadm_all(iscsi_command, **kwargs)

    def _run_nscli_wrap(self, args, sp_state, check_exit_code=None,
                        cmd_type=None, **kwargs):
        """_run_nscli wrapper, SP abnormality is detected and reexecuted."""
        sp_state = self._check_db_ctrl_info(sp_state)
        pre_sp = self.storage_processor
        args = self._check_arrange_cmd(args, cmd_type)

        try:
            out = self._run_nscli(args, check_exit_code)
        except exception.ProcessExecutionError as e:
            if e.stderr is utils.PROCESS_TIMEOUT:
                # Relevant SP is renewed in failure.
                sp_state[pre_sp] = False
                if (sp_state['a'] == False and sp_state['b'] == False):
                    LOG.error(_("No replies from EMC StorageProcessor."))
                    raise e
                else:
                    # Error by time-out and reexecution
                    LOG.debug("Timeout error, Change SP exec command.")
                    if pre_sp == self.storage_processor:
                        self._change_nscli_ctrl_info()
                        if pre_sp == self.storage_processor:
                            # Failing SP change, second command isn't done.
                            raise e
                    if cmd_type is not None:
                        check_res = self._sub_check_for_nscli_exec(
                                                   cmd_type, **kwargs)
                        if check_res == False:
                            return ('', sp_state)
                        args = self._check_arrange_cmd(args, cmd_type)
                    return (self._run_nscli(args, check_exit_code), sp_state)
            else:
                raise e
        return (out, sp_state)

    def _run_nscli(self, args, check_exit_code=None):
        cmd = None
        if (self.storage_processor == 'a'):
            cmd = [FLAGS.emc_nscli_cmd,
                   '-h', FLAGS.emc_nscli_ip,
                   '-port', FLAGS.emc_nscli_port,
                   '-user', FLAGS.emc_nscli_login,
                   '-password', FLAGS.emc_nscli_password,
                   '-scope', FLAGS.emc_nscli_scope]
        else:
            cmd = [FLAGS.emc_nscli_cmd,
                   '-h', FLAGS.emc_nscli_ip2,
                   '-port', FLAGS.emc_nscli_port2,
                   '-user', FLAGS.emc_nscli_login2,
                   '-password', FLAGS.emc_nscli_password2,
                   '-scope', FLAGS.emc_nscli_scope]
        cmd.extend(args)
        cmd = map(str, cmd)

        if check_exit_code is None:
            check_exit_code = [0]
        (out, _err) = self._execute(*cmd,
                                    check_exit_code=check_exit_code,
                                    timeout=FLAGS.emc_nscli_exec_timeout)
        return out

    def _ensure_emc_alus(self, ctxt, host):
        """Ensure that target ids have been created in datastore."""
        alus = self.db.emc_alu_count_by_host(ctxt, host,
                                             self.min_alu, self.max_alu)
        if alus >= self.max_alu - self.min_alu + 1:
            return
        for alu_num in xrange(self.min_alu, self.max_alu + 1):
            target = {'host': host, 'alu': alu_num}
            self.db.emc_alu_create_safe(ctxt, target)

    def _virtual_disk_exists(self, alu, sp_state):
        try:
            args = ['getlun', alu, '-name']
            (out, sp_state) = self._run_nscli_wrap(args, sp_state)
        except exception.ProcessExecutionError as e:
            if e.exit_code == 30:
                return (False, sp_state)
            raise e
        return (True, sp_state)

    def _check_used_lun(self, ctxt, host, sp_state):
        args = ['getlun', '-name']
        (out, sp_state) = self._run_nscli_wrap(args, sp_state)
        for result in out.splitlines():
            if result.startswith("LOGICAL UNIT NUMBER"):
                lun = int(result.split(' ')[-1])
                if (self.min_alu <= lun <= self.max_alu):
                    self.db.emc_alu_disabled(ctxt, lun, host)
        return sp_state

    def _check_exist_lun(self, cmd, lunname):
        """LUN existence check processing"""
        out = self._run_nscli(cmd, check_exit_code=[0, 30])
        for result in out.splitlines():
            if lunname in result:
                return True
        return False

    def _check_exist_sg_hlu(self, cmd, hlu, alu):
        """Storage group's hlu existence check processing"""
        check_target = [hlu, alu]
        check_target = map(str, check_target)
        out = self._run_nscli(cmd)
        for result in out.splitlines():
            if check_target == result.split():
                return True
        return False

    def _check_exist_sg_host(self, cmd, host):
        """Storage group's host existence check processing"""
        out = self._run_nscli(cmd)
        for result in out.splitlines():
            if host in result:
                return True
        return False

    def _check_exist_sg(self, cmd):
        """Storage group existence check processing"""
        check_word = 'Storage Group Name'
        out = self._run_nscli(cmd, check_exit_code=[0, 83])
        for result in out.splitlines():
            if check_word in result:
                return True
        return False

    def _check_exist_sg_snapshot(self, cmd, snapname, hlu=-1):
        """Storage group's snapshot existence check processing"""
        out = self._run_nscli(cmd)
        for result in out.splitlines():
            if snapname in result:
                if hlu == -1:
                    return True
                else:
                    res_list = result.split()
                    if res_list[0] == str(hlu):
                        return True
        return False

    def _check_exist_snapshot(self, cmd):
        """Snapshot existence check processing"""
        check_word = 'SnapView logical unit name'
        out = self._run_nscli(cmd, check_exit_code=[0, 1])
        for result in out.splitlines():
            if check_word in result:
                return True
        return False

    def _check_activate_snapshot(self, cmd):
        """Active snapshot existence check processing"""
        check_word = 'Session Name'
        out = self._run_nscli(cmd, check_exit_code=[0, 1])
        for result in out.splitlines():
            if check_word in result:
                return True
        return False

    def _check_exist_session(self, cmd):
        """Session existence check processing"""
        check_session_word = 'Name of the session'
        out = self._run_nscli(cmd, check_exit_code=[0, 151])
        for result in out.splitlines():
            if check_session_word in result:
                return True
        return False

    def _check_exist_clonegroup(self, cmd):
        check_session_word = 'CloneGroup is not available'
        out = self._run_nscli(cmd, check_exit_code=[0, 69])
        if not check_session_word in out:
                return True
        return False

    def _check_exist_clone_by_alu(self, cmd, d_alu):
        out = self._run_nscli(cmd, check_exit_code=[0, 69])
        if str(d_alu) in out:
                return True
        return False

    def _check_fracture_clone(self, cmd):
        out = self._run_nscli(cmd, check_exit_code=[0, 70])
        if not 'Synchronizing' in out:
                return True
        return False

    def _check_exist_clone_by_cloneid(self, cmd):
        check_session_word = 'Clone is not found'
        out = self._run_nscli(cmd, check_exit_code=[0, 70])
        if not check_session_word in out:
                return True
        return False

    def _sub_check_for_nscli_exec(self, cmd_type, **kwargs):
        """Double issue confirmation processing of the nscli command."""
        cmd = copy.deepcopy(EXEC_CHECK_CMD_TABLE[cmd_type])
        if cmd_type is 'lun_create':
            cmd.insert(1, kwargs.pop('alu'))
            if self._check_exist_lun(cmd, kwargs.pop('lunname')):
                return False
        elif cmd_type is 'lun_destroy':
            cmd.insert(1, kwargs.pop('alu'))
            if self._check_exist_lun(cmd, kwargs.pop('lunname')) == False:
                return False
        elif cmd_type is 'sg_add_hlu':
            cmd.insert(3, kwargs.pop('gname'))
            if self._check_exist_sg_hlu(cmd, kwargs.pop('hlu'),
                                        kwargs.pop('alu')):
                return False
        elif cmd_type is 'sg_remove_hlu':
            cmd.insert(3, kwargs.pop('gname'))
            if self._check_exist_sg_hlu(cmd, kwargs.pop('hlu'),
                                        kwargs.pop('alu')) == False:
                return False
        elif cmd_type is 'sg_disconnect_host':
            cmd.insert(3, kwargs.pop('gname'))
            if self._check_exist_sg_host(cmd, kwargs.pop('host')) == False:
                return False
        elif cmd_type is 'sg_destroy':
            cmd.insert(3, kwargs.pop('gname'))
            if self._check_exist_sg(cmd) == False:
                return False
        elif cmd_type is 'sg_add_snapshot':
            cmd.insert(3, kwargs.pop('gname'))
            if self._check_exist_sg_snapshot(cmd,
                                        kwargs.pop('snapname'),
                                        hlu=kwargs.pop('hlu')):
                return False
        elif cmd_type is 'sg_remove_snapshot':
            cmd.insert(3, kwargs.pop('gname'))
            if self._check_exist_sg_snapshot(cmd,
                                        kwargs.pop('snapname')) == False:
                return False
        elif cmd_type is 'snapshot_create':
            cmd.insert(4, kwargs.pop('snapname'))
            if self._check_exist_snapshot(cmd):
                return False
        elif cmd_type is 'snapshot_activate':
            cmd.insert(4, kwargs.pop('snapname'))
            if self._check_activate_snapshot(cmd):
                return False
        elif cmd_type is 'snapshot_deactivate':
            cmd.insert(4, kwargs.pop('snapname'))
            if self._check_activate_snapshot(cmd) == False:
                return False
        elif cmd_type is 'snapshot_remove':
            cmd.insert(4, kwargs.pop('snapname'))
            if self._check_exist_snapshot(cmd) == False:
                return False
        elif cmd_type is 'start_session':
            cmd.insert(4, kwargs.pop('sessionname'))
            if self._check_exist_session(cmd):
                return False
        elif cmd_type is 'stop_session':
            cmd.insert(4, kwargs.pop('sessionname'))
            if self._check_exist_session(cmd) == False:
                return False
        elif cmd_type is 'clone_list_clonegroup':
            return True
        elif cmd_type is 'clone_list_clone':
            return True
        elif cmd_type is 'clone_create_clonegroup':
            cmd.insert(3, kwargs.pop('name'))
            if not self._check_exist_clonegroup(cmd):
                return False
            return True
        elif cmd_type is 'clone_add_clone':
            cmd.insert(3, kwargs.pop('name'))
            d_alu = kwargs.pop('luns')
            if not self._check_exist_clone_by_alu(cmd, d_alu):
                return False
            return True
        elif cmd_type is 'clone_fracture_clone':
            cmd.insert(3, kwargs.pop('name'))
            cmd.insert(5, kwargs.pop('cloneid'))
            if not self._check_fracture_clone(cmd):
                return False
            return True
        elif cmd_type is 'clone_remove_clone':
            cmd.insert(3, kwargs.pop('name'))
            cmd.insert(5, kwargs.pop('cloneid'))
            if self._check_exist_clone_by_cloneid(cmd):
                return False
            return True
        elif cmd_type is 'clone_destroy_clonegroup':
            cmd.insert(3, kwargs.pop('name'))
            if self._check_exist_clonegroup(cmd):
                return False
            return True
        return True

    def _check_db_ctrl_info(self, sp_state):
        """Renewal judgment of DB maintenance SP"""
        db_sp = self.db.emc_get_current_sp(self.admin_context, self.host)
        if db_sp['current_sp'] != self.storage_processor:
            LOG.info(_("Renewal of DB preservation SP was confirmed."))
            self._change_nscli_ctrl_info()
            if db_sp['current_sp'] == self.storage_processor:
                # SP change was performed, make relevant SP return.
                if sp_state[self.storage_processor] == False:
                    sp_state[self.storage_processor] = True
        return sp_state

    def _check_arrange_cmd(self, args, cmd_type):
        """Parameter arrangement along the command-type"""
        if cmd_type is None:
            pass
        elif cmd_type == 'lun_create':
            #Renewal of designation SP
            index_sp = args.index('-sp') + 1
            args[index_sp] = self.storage_processor
        return args

    def _create_lun(self, volume_id, size, sp_state):
        """Creates a LUN for a volume or a snapshot"""

        alu = 0
        ctxt = self.admin_context
        target_count = self.db.emc_alu_count(ctxt, self.host,
                                             self.min_alu, self.max_alu)
        if target_count >= 1:
            alu = self.db.emc_alu_allocate(ctxt, self.host, volume_id,
                                           self.min_alu, self.max_alu,
                                           FLAGS.emc_alu_reuse_interval)
            args = ['lun',
                    '-create',
                    '-capacity', size,
                    '-sq', 'gb',
                    '-poolId', FLAGS.emc_nscli_pool_id,
                    '-sp', FLAGS.emc_nscli_storage_processor,
                    '-l', alu,
                    '-name', volume_id]
            (out, sp_state) = self._run_nscli_wrap(args, sp_state,
                                                cmd_type='lun_create',
                                                alu=alu,
                                                lunname=volume_id)
        else:
            raise exception.EmcNoMoreFreeEntry(table='alu', host=self.host)
        return (alu, sp_state)

    def _destroy_lun(self, alu, size, volume_id, sp_state, force=False):
        """Clears data on the LUN and destroys it."""
        ctxt = self.admin_context
        (result, sp_state) = self._virtual_disk_exists(alu, sp_state)
        if result:
            # Delete data on the LUN
            self._ensure_emc_hlus(ctxt, self.host)
            hlu = self.db.emc_hlu_allocate(ctxt, self.host, self.host, alu,
                                           self.min_hlu, self.max_hlu)
            sp_state = self._set_permission(hlu, alu, self.host, sp_state)
            try:
                # Connect to iSCSI target
                path = self._connect_iscsi(hlu, alu)
                # Write null character to path
                with utils.temporary_chown(path):
                    self._execute('cstream', '-i', '-',
                                    '-o', path,
                                    '-n', '%dg' % int(size),
                                    run_as_root=True)
            finally:
                # Disconnect to iSCSI target
                sp_state = self._disconnect_iscsi(hlu, sp_state,
                                                  force=force)

            # Delete the LUN
            args = ['lun', '-destroy', '-l', alu, '-o']
            (out, sp_state) = self._run_nscli_wrap(args, sp_state,
                                            cmd_type='lun_destroy',
                                            alu=alu,
                                            lunname=volume_id)
        return sp_state

    def _set_permission(self, hlu, alu, host, sp_state):
        """Set permission for virtual disk access"""
        ctxt = self.admin_context
        try:
            args = ['storagegroup', '-addhlu',
                    '-gname', host,
                    '-hlu', hlu,
                    '-alu', alu]
            (out, sp_state) = self._run_nscli_wrap(args, sp_state,
                                                cmd_type='sg_add_hlu',
                                                gname=host,
                                                hlu=hlu,
                                                alu=alu)
        except Exception as e:
            if e.exit_code == 66:
                # Error returned from Agent
                # Requested LUN has already been added to this Storage Group
                args = ['storagegroup', '-list',
                        '-gname', host]
                out = self._run_nscli(args)
                hlu_alu_info = False
                for result in out.splitlines():
                    params = result.strip().split()
                    if params == ['HLU', 'Number', 'ALU', 'Number']:
                        hlu_alu_info = True
                        continue
                    if hlu_alu_info:
                        if params == [str(hlu), str(alu)]:
                            LOG.debug("Pre-allocated ALU/HLU pair found.")
                            return
                else:
                    LOG.error("Another pre-allocated ALU/HLU pair found.")

            self.db.emc_hlu_deallocate(ctxt, self.host, hlu, host)
            raise e
        return sp_state

    def _revoke_permission(self, hlu, host, sp_state):
        """Revoke permission for virtual disk access"""
        ctxt = self.admin_context
        a_lun = self.db.emc_alu_get_by_hlu_and_host(ctxt, self.host,
                                                    hlu, host)
        args = ['storagegroup', '-removehlu',
                '-gname', host,
                '-hlu', hlu,
                '-o']
        (out, sp_state) = self._run_nscli_wrap(args, sp_state,
                                        check_exit_code=[0, 66],
                                        cmd_type='sg_remove_hlu',
                                        gname=host,
                                        hlu=hlu,
                                        alu=a_lun['alu'])
        self.db.emc_hlu_deallocate(ctxt, self.host, hlu, host)
        return sp_state

    @lockutils.synchronized('emc-multipath', 'cinder-')
    def _connect_iscsi(self, hlu, alu):
        ctxt = self.admin_context
        target_total_num = len(self.target_iqn)
        failed_count = 0
        last_exc = None
        for target_num in range(target_total_num):
            try:
                self._run_iscsiadm_one(target_num, ())
            except exception.ProcessExecutionError as exc:
                if exc.exit_code in [255]:
                    try:
                        self._run_iscsiadm_one(target_num, ('--op', 'new'))
                    except exception.ProcessExecutionError as exc:
                        failed_count += 1
                        last_exc = exc
                else:
                    failed_count += 1
                    last_exc = exc

        if failed_count == target_total_num:
            # failed to all target
            raise last_exc

        failed_count = 0
        last_exc = None
        loggedin_target = []
        (_out, err) = self._execute('iscsiadm', '-m', 'session',
                                   run_as_root=True,
                                   check_exit_code=[0, 255])
        for target_num in range(target_total_num):
            if (self.target_iqn[target_num] + "\n") in _out:
                loggedin_target.append(target_num)
            else:
                try:
                    self._run_iscsiadm_one(target_num, ("--login",))
                except exception.ProcessExecutionError as exc:
                    failed_count += 1
                    last_exc = exc

        if failed_count == target_total_num:
            # failed to all target
            raise last_exc

        self._iscsiadm_update("node.startup", "automatic")

        failed_count = 0
        last_exc = None

        # The processing to make host_devices recognition
        for target_num in loggedin_target:
            try:
                self._run_iscsiadm_one(target_num, ("--rescan",))
            except exception.ProcessExecutionError as exc:
                failed_count += 1
                last_exc = exc

        if failed_count == target_total_num:
            raise last_exc

        tries = 0
        existing_devices = None
        while True:
            self._execute('udevadm', 'settle',
                          '--timeout=%s' % FLAGS.emc_nscli_udev_timeout,
                          run_as_root=True,
                          check_exit_code=[0, 1])

            existing_devices = self._get_existing_host_devices(hlu)
            if existing_devices:
                break

            if tries >= FLAGS.num_iscsi_scan_tries:
                raise exception.CinderException(_(
                                      "iSCSI device not found"))

            LOG.info(_("ISCSI volume not yet found."
                       "Will rescan & retry.  Try number: %(tries)s") %
                     locals())
            tries = tries + 1
            time.sleep(tries ** 2)

        # Change to multipath from host device path
        multipath_full_path = self._change_multipath(existing_devices)

        # returns real path of host_device
        return os.path.realpath(multipath_full_path)

    @lockutils.synchronized('emc-multipath', 'cinder-')
    def _disconnect_iscsi(self, hlu, sp_state, force=False):
        chk_code = {}
        if force is True:
            chk_code['force'] = False
        ctxt = self.admin_context

        # When it can be connected to one of targets, multipath can be made.
        # Therefore only the one which exists is used.
        existing_devices = self._get_existing_host_devices(hlu)
        if not existing_devices:
            raise exception.CinderException(_("iSCSI device not found"))

        self._execute('udevadm', 'settle',
                      '--timeout=%s' % FLAGS.emc_nscli_udev_timeout,
                      run_as_root=True,
                      check_exit_code=chk_code.get('force', [0]))

        # multipath_name is used at the time of elimination of multipath.
        link_path = []
        link_path_state = {}
        for i_device in existing_devices:
            link_path.append(os.readlink(i_device))

        try:
            link_path_state, multipath_name = \
                 self._get_multipath_name_and_status(link_path)

            # Flush page-cache before eliminate multipath.
            multipath_full_path = '/dev/mapper/' + multipath_name
            real_multi_path = os.path.realpath(multipath_full_path)
            self._execute('blockdev', '--flushbufs', real_multi_path,
                          run_as_root=True,
                          check_exit_code=chk_code.get('force', [0]))
            # multipath is eliminated.
            (out, err) = self._execute('multipath', '-f', multipath_name,
                                       run_as_root=True,
                                       attempts=FLAGS.multipath_remove_retries,
                                       check_exit_code=chk_code.get('force', [0]),
                                       timeout=FLAGS.multipath_cli_exec_timeout)
        except Exception as exc:
            if force is False:
                raise exc

        # Remove the symbolic link for iSCSI target.
        path_is_alive = False
        for i_device in existing_devices:
            # If "the path is not active", proceed flush and delete.
            device_link = os.readlink(i_device).rsplit('/', 1)[1]
            path_simlink_state = link_path_state.get(device_link, False)
            # don't check_exit_code if the path is dead
            i_chk_code = path_simlink_state
            self._execute('blockdev', '--flushbufs', i_device,
                          run_as_root=True,
                          check_exit_code=chk_code.get('force', i_chk_code))
            if path_simlink_state is True:
                i_chk_code = [0, 2]
                path_is_alive = True
            (_drivepath, drivename) = os.path.split(os.path.realpath(i_device))
            sysfs_path = "/sys/block/%s/device/delete" % drivename
            if os.path.exists(sysfs_path):
                self._execute('dd', 'of=%s' % sysfs_path,
                              process_input='1', run_as_root=True,
                              check_exit_code=chk_code.get('force', i_chk_code))

        if not path_is_alive and not force:
            raise exception.CinderException(
                _("all path of %s were not active.") % (multipath_name))

        # Remove an access premission to iSCSI target.
        sp_state = self._revoke_permission(hlu, self.host, sp_state)

        return sp_state

    def _init_storagegroup_registration(self, initiator, host, sp_state):
        """Storage Group is made and host registration is done."""
        # Create storagegroup. Error for SG already made is skipped.
        args = ['storagegroup',
                '-create',
                '-gname', host]
        (out, sp_state) = self._run_nscli_wrap(args, sp_state,
                                        check_exit_code=[0, 66])
        # Connect host to storagegroup.
        args = ['storagegroup',
                '-connecthost',
                '-host', initiator,
                '-gname', host,
                '-o']
        (out, sp_state) = self._run_nscli_wrap(args, sp_state)
        return sp_state

    def _destroy_storagegroup_registration(self, initiator, host, sp_state):
        """Host registration is released and a storage group is broken."""
        # Disconnect host to storagegroup
        args = ['storagegroup',
                '-disconnecthost',
                '-host', initiator,
                '-o']
        (out, sp_state) = self._run_nscli_wrap(args, sp_state,
                                        cmd_type='sg_disconnect_host',
                                        gname=host,
                                        host=initiator)
        # Destroy storagegroup
        args = ['storagegroup',
                '-destroy',
                '-gname', host,
                '-o']
        (out, sp_state) = self._run_nscli_wrap(args, sp_state,
                                        cmd_type='sg_destroy',
                                        gname=host)
        return sp_state

    def _check_busy_storagegroup_count(self, sp_state):
        """The storage group which exists already is counted."""
        count = 0
        args = ['storagegroup',
                '-list']
        (out, sp_state) = self._run_nscli_wrap(args, sp_state)
        for result in out.splitlines():
            if "Storage Group Name" in result:
                count += 1
        return (count, sp_state)

    def _connect_to_ssh(self, connect_ip):
        """A connection in SSH is begun."""
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        privatekeyfile = os.path.expanduser(FLAGS.emc_nscli_ssh_private_key)
        # It sucks that paramiko doesn't support DSA keys
        privatekey = paramiko.RSAKey.from_private_key_file(privatekeyfile)
        ssh.connect(connect_ip,
                    port=FLAGS.emc_nscli_ssh_port,
                    username=FLAGS.emc_nscli_ssh_login,
                    pkey=privatekey)
        return ssh

    def check_for_setup_error(self):
        """Returns an error if prerequisites aren't met"""
        if not (FLAGS.emc_nscli_ip) and not (FLAGS.emc_nscli_ip2):
            raise exception.FlagNotSet(flag="emc_nscli_ip and emc_nscli_ip2")

        if not (FLAGS.emc_nscli_service_ip):
            raise exception.FlagNotSet(flag="emc_nscli_service_ip")

        if not (FLAGS.emc_nscli_target_iqn):
            raise exception.FlagNotSet(flag="emc_nscli_target_iqn")

        if FLAGS.emc_nscli_ip:
            if not (FLAGS.emc_nscli_login):
                raise exception.FlagNotSet(flag="emc_nscli_login")

            if not (FLAGS.emc_nscli_password):
                raise exception.FlagNotSet(flag="emc_nscli_password")

        if FLAGS.emc_nscli_ip2:
            if not (FLAGS.emc_nscli_login2):
                raise exception.FlagNotSet(flag="emc_nscli_login2")

            if not (FLAGS.emc_nscli_password2):
                raise exception.FlagNotSet(flag="emc_nscli_password2")

        if not (FLAGS.emc_nscli_ssh_private_key):
            raise exception.FlagNotSet(flag="emc_nscli_ssh_private_key")

        ctxt = self.admin_context
        sp_state = {'a': True, 'b': True}
        init_sp = self.db.emc_get_current_sp(ctxt, self.host)
        if init_sp is None:
            self.db.emc_set_current_sp(ctxt, self.host,
                                        FLAGS.emc_nscli_storage_processor)
        elif init_sp['current_sp'] != self.storage_processor:
            self._change_nscli_ctrl_info(init=True)

        try:
            args = ["storagepool", "-list", "-id",
                    FLAGS.emc_nscli_pool_id]
            (out, sp_state) = self._run_nscli_wrap(args, sp_state)
        except exception.ProcessExecutionError as e:
            if e.exit_code == 72:
                raise exception.StoragePoolNotFound(
                                storage_pool=FLAGS.emc_nscli_pool_id)
            if e.exit_code == 255:
                raise exception.Error(_('Authentication failed'))
            raise e

        self._ensure_emc_alus(ctxt, self.host)
        sp_state = self._check_used_lun(ctxt, self.host, sp_state)
        # Check Login ISCSI target
        (out_s, err) = self._execute('iscsiadm', '-m', 'session',
                                     run_as_root=True,
                                     check_exit_code=[0, 255])
        failed_count = 0
        last_exc = None
        target_total_num = len(self.target_portal)
        # Repeat Login for ISCSI target
        for i in range(target_total_num):
            target_portal = self.target_portal[i]
            target_ip = target_portal.split(':')[0]
            if not (self.target_iqn[i] + "\n") in out_s:
                try:
                    cmd = ['iscsiadm', '-m', 'discovery', '-t', 'st',
                           '-p', target_ip]
                    cmd = map(str, cmd)
                    (out, err) = self._execute(*cmd,
                                               run_as_root=True)
                    LOG.debug("iscsiadm discovery %s: stdout=%s stderr=%s" %
                            (cmd, out, err))
                    cmd = ['iscsiadm', '-m', 'node', '-T',
                           self.target_iqn[i],
                           '--login']
                    cmd = map(str, cmd)
                    (out, err) = self._execute(*cmd,
                                               run_as_root=True,
                                               check_exit_code=[0, 255])
                    LOG.debug("iscsiadm login %s: stdout=%s stderr=%s" %
                            (cmd, out, err))
                except exception.ProcessExecutionError as exc:
                    failed_count += 1
                    last_exc = exc
        if failed_count == target_total_num:
            # failed to all target
            raise last_exc
        # Storage Group is registered
        initiator_iqn = volume_utils.get_iscsi_initiator()
        sp_state = self._init_storagegroup_registration(
                                        initiator_iqn, self.host, sp_state)

    def create_volume(self, volume):
        """Creates a volume."""

        sp_state = {'a': True, 'b': True}
        (alu, sp_state) = self._create_lun(
                                        volume['id'],
                                        volume['size'],
                                        sp_state)
        ctxt = self.admin_context

        iscsi_portal_interface = '1'
        iscsi_portal = "%s:%d,%s" % (FLAGS.emc_nscli_service_ip,
                       FLAGS.emc_nscli_service_port, iscsi_portal_interface)
        db_update = dict(
            provider_location="%s %s %d" % (iscsi_portal,
                                            FLAGS.emc_nscli_target_iqn,
                                            alu))
        return db_update

    def ensure_export(self, ctxt, volume):
        pass

    def create_export(self, ctxt, volume):
        """Synchronously recreates an export for a logical volume."""
        pass

    def delete_volume(self, volume, force=False):
        """Deletes a volume."""
        # If the volume has snapshot(s), we can't delete it.
        if self._has_snapshot(volume):
            raise exception.VolumeIsBusy(volume_name=volume['name'])

        sp_state = {'a': True, 'b': True}
        ctxt = self.admin_context
        vdisk = self.db.emc_alu_get_by_volume_id(ctxt, volume['id'])
        if vdisk is None:
            return
        alu = vdisk['alu']
        sp_state = self._destroy_lun(alu, volume['size'],
                                    volume['id'], sp_state,
                                    force=force)
        self.db.emc_alu_deallocate(ctxt, self.host, alu)

    def remove_export(self, ctxt, volume):
        """Removes an export for a logical volume."""
        pass

    def initialize_connection(self, volume, connector):
        @lockutils.synchronized('emc-connection' + connector['host'],
                                'emc-connection-')
        def do_initialize_connection():
            ssh = None
            ctxt = self.admin_context
            sp_state = {'a': True, 'b': True}
            # A storage group a self-host possesses is checked.
            if self.db.emc_hlu_count_used(ctxt, self.host, connector['host'],
                                          self.min_hlu, self.max_hlu) == 0:
                # Check busy storage group count.
                (busy_sg_count, sp_state) = \
                    self._check_busy_storagegroup_count(sp_state)
                if busy_sg_count >= FLAGS.emc_nscli_max_storage_group_count:
                    raise exception.Error(_(
                        'Number of storage groups from the storage exceeded, '
                        + 'busy number is %d. ') % (busy_sg_count))
                try:
                    failed_count = 0
                    last_exc = None
                    target_total_num = len(self.target_portal)
                    # ssh connect to nova-compute.
                    ssh = self._connect_to_ssh(connector['ip'])
                    args = ['sudo', 'iscsiadm', '-m', 'session']
                    args = " ".join(map(str, args))
                    (out_s, err) = utils.ssh_execute(ssh, args,
                                                     check_exit_code=[0, 21])
                    # Repeat Login for ISCSI target
                    for i in range(target_total_num):
                        target_portal = self.target_portal[i]
                        target_ip = target_portal.split(':')[0]
                        if not (self.target_iqn[i] + "\n") in out_s:
                            try:
                                args = ['sudo', 'iscsiadm', '-m', 'discovery',
                                        '-t', 'st', '-p', target_ip]
                                args = " ".join(map(str, args))
                                (out, err) = utils.ssh_execute(ssh, args)
                                LOG.debug(
                                         "ssh iscsiadm %s: stdout=%s stderr=%s"
                                         % (args, out, err))
                                args = ['sudo', 'iscsiadm', '-m', 'node',
                                        '-T', self.target_iqn[i],
                                        '--login']
                                args = " ".join(map(str, args))
                                (out, err) = utils.ssh_execute(ssh, args,
                                                 check_exit_code=[0, 255])
                                LOG.debug(
                                         "ssh iscsiadm %s: stdout=%s stderr=%s"
                                         % (args, out, err))
                            except exception.ProcessExecutionError as exc:
                                failed_count += 1
                                last_exc = exc
                    if failed_count == target_total_num:
                        # failed to all target
                        raise last_exc
                    # Storage Group is registered
                    sp_state = self._init_storagegroup_registration(
                                                     connector['initiator'],
                                                     connector['host'],
                                                     sp_state)
                finally:
                    if ssh:
                        ssh.close()
            self._ensure_emc_hlus(ctxt, connector['host'])
            iscsi_properties = self._get_iscsi_properties(volume)
            alu = iscsi_properties['target_lun']

            h_lun = self.db.emc_hlu_get_by_alu_and_host(ctxt, self.host, alu,
                                                        connector['host'])
            if h_lun:
                hlu = h_lun['hlu']
            else:
                hlu = self.db.emc_hlu_allocate(ctxt, self.host,
                                               connector['host'], alu,
                                               self.min_hlu, self.max_hlu)
                sp_state = self._set_permission(hlu, alu, connector['host'],
                                               sp_state)

            iscsi_properties['target_lun'] = int(hlu)
            return {
                'driver_volume_type': 'emc_iscsi',
                'data': iscsi_properties
            }
        return do_initialize_connection()

    def terminate_connection(self, volume, connector):
        @lockutils.synchronized('emc-connection' + connector['host'],
                                'emc-connection-')
        def do_terminate_connection():
            ssh = None
            ctxt = self.admin_context
            sp_state = {'a': True, 'b': True}
            iscsi_properties = self._get_iscsi_properties(volume)
            h_lun = self.db.emc_hlu_get_by_alu_and_host(ctxt, self.host,
                        iscsi_properties['target_lun'], connector['host'])
            if h_lun is None:
                # Volume already detached, perhaps VM shutdown or destroy.
                return
            sp_state = self._revoke_permission(h_lun['hlu'],
                                            connector['host'],
                                            sp_state)
            # A storage group a self-host possesses is checked.
            if self.db.emc_hlu_count_used(ctxt, self.host, connector['host'],
                                          self.min_hlu, self.max_hlu) == 0:
                try:
                    # ssh connect to nova-compute.
                    ssh = self._connect_to_ssh(connector['ip'])
                    # A storage group is broken.
                    sp_state = self._destroy_storagegroup_registration(
                                                        connector['initiator'],
                                                        connector['host'],
                                                        sp_state)
                    # Repeat Logout for iSCSI connect target.
                    # An exception of logout processing is ignored.
                    for i in range(len(self.target_portal)):
                        args = ['sudo', 'iscsiadm', '-m', 'node',
                                '-T', iscsi_properties['target_iqn'][i],
                                '--logout']
                        args = " ".join(map(str, args))
                        (out, err) = utils.ssh_execute(ssh, args,
                                                       check_exit_code=False)
                        LOG.debug("ssh iscsiadm %s: stdout=%s stderr=%s" %
                                 (args, out, err))
                finally:
                    if ssh:
                        ssh.close()
        return do_terminate_connection()

    @lockutils.synchronized('emc-snapshot', 'cinder-')
    def create_volume_from_snapshot(self, volume, snapshot):
        """Creates a volume from snapshot reference."""
        ctxt = self.admin_context
        self._ensure_emc_hlus(ctxt, self.host)
        sp_state = {'a': True, 'b': True}

        # Get alu of source LUN
        s_vol = self.db.volume_get(ctxt, snapshot['volume_id'])
        iscsi_properties = self._get_iscsi_properties(s_vol)
        s_alu = iscsi_properties['target_lun']

        # Get hlu of snapshot LUN
        s_hlu = self.db.emc_hlu_allocate(ctxt, self.host, self.host, s_alu,
                                         self.min_hlu, self.max_hlu)

        # Register hlu of snapshot LUN to storagegroup
        snapshot_name = self._build_snapshot_name(snapshot)
        args = ['storagegroup', '-addsnapshot',
                '-gname', self.host,
                '-hlu', s_hlu,
                '-snapshotname', snapshot_name]
        (out, sp_state) = self._run_nscli_wrap(args, sp_state,
                                        cmd_type='sg_add_snapshot',
                                        gname=self.host,
                                        hlu=s_hlu,
                                        snapname=snapshot_name)

        # Create new LUN and Get alu of new LUN
        (d_alu, sp_state) = self._create_lun(volume['id'],
                                            volume['size'],
                                            sp_state)
        d_hlu = self.db.emc_hlu_allocate(ctxt, self.host, self.host, d_alu,
                                         self.min_hlu, self.max_hlu)
        sp_state = self._set_permission(d_hlu, d_alu, self.host, sp_state)

        try:
            # Connect to iSCSI target
            s_path = self._connect_iscsi(s_hlu, s_alu)
            d_path = self._connect_iscsi(d_hlu, d_alu)

            # Copy a snapshot LUN to new LUN
            with utils.temporary_chown(s_path):
                with utils.temporary_chown(d_path):
                    self._execute('cstream',
                                    '-i', s_path,
                                    '-o', d_path,
                                    '-n', '%dg' % int(volume['size']),
                                    run_as_root=True)
        finally:
            # Disconnect to iSCSI target
            sp_state = self._disconnect_iscsi(d_hlu, sp_state)
            sp_state = self._disconnect_iscsi(s_hlu, sp_state)

        iscsi_portal_interface = '1'
        iscsi_portal = "%s:%d,%s" % (FLAGS.emc_nscli_service_ip,
                       FLAGS.emc_nscli_service_port, iscsi_portal_interface)

        db_update = {}
        db_update['provider_location'] = ("%s %s %d" % (iscsi_portal,
                                          FLAGS.emc_nscli_target_iqn, d_alu))
        return db_update

    def create_snapshot(self, snapshot):
        """Creates a snapshot of a logical volume."""
        ctxt = self.admin_context
        sp_state = {'a': True, 'b': True}
        # Check whether the number of snapshots is greater than the upper limit
        snapshot_num = self.db.snapshot_get_count_by_volume_id(
                           ctxt,
                           snapshot['volume_id'])
        if snapshot_num > FLAGS.emc_nscli_max_snapshot_count:
            raise exception.Error(
                _('Number of snapshots from a volume exceeded'))

        # Get alu of LUN
        vol = self.db.volume_get(ctxt, snapshot['volume_id'])
        iscsi_properties = self._get_iscsi_properties(vol)
        alu = iscsi_properties['target_lun']

        snapshot_name = self._build_snapshot_name(snapshot)
        session_name = self._build_snapshot_session_name(snapshot)

        # Create snapshot
        args = ['snapview',
                '-createsnapshot', alu,
                '-snapshotname', snapshot_name]
        (out, sp_state) = self._run_nscli_wrap(args, sp_state,
                                            cmd_type='snapshot_create',
                                            snapname=snapshot_name)
        # Start snapshot session
        args = ['snapview',
                '-startsession', session_name,
                '-snapshotname', snapshot_name]
        (out, sp_state) = self._run_nscli_wrap(args, sp_state,
                                            cmd_type='start_session',
                                            sessionname=session_name)
        # Activate snapshot
        args = ['snapview',
                '-activatesnapshot', session_name,
                '-snapshotname', snapshot_name]
        (out, sp_state) = self._run_nscli_wrap(args, sp_state,
                                            cmd_type='snapshot_activate',
                                            snapname=snapshot_name)

    def delete_snapshot(self, snapshot):
        """Removes a snapshot of a logical volume."""
        snapshot_name = self._build_snapshot_name(snapshot)
        session_name = self._build_snapshot_session_name(snapshot)
        sp_state = {'a': True, 'b': True}

        # Deactivate snapshot
        args = ['snapview',
                '-deactivatesnapshot',
                '-snapshotname', snapshot_name,
                '-o']
        (out, sp_state) = self._run_nscli_wrap(args, sp_state,
                                            check_exit_code=[0, 24, 154],
                                            cmd_type='snapshot_deactivate',
                                            snapname=snapshot_name)
        # Stop snapshot session
        args = ['snapview',
                '-stopsession', session_name,
                '-o']
        (out, sp_state) = self._run_nscli_wrap(args, sp_state,
                                            check_exit_code=[0, 151],
                                            cmd_type='stop_session',
                                            sessionname=session_name)
        # Remove snapshot
        args = ['snapview',
                '-rmsnapshot',
                '-snapshotname', snapshot_name,
                '-o']
        (out, sp_state) = self._run_nscli_wrap(args, sp_state,
                                            check_exit_code=[0, 154],
                                            cmd_type='snapshot_remove',
                                            snapname=snapshot_name)

    def copy_image_to_volume(self, ctxt, volume, image_service, image_id):
        actxt = self.admin_context
        sp_state = {'a': True, 'b': True}

        # Set an access premission to iSCSI target.
        volume = self.db.volume_get(actxt, volume['id'])
        self._ensure_emc_hlus(actxt, self.host)
        iscsi_properties = self._get_iscsi_properties(volume)
        alu = iscsi_properties['target_lun']
        hlu = self.db.emc_hlu_allocate(actxt, self.host, self.host, alu,
                                       self.min_hlu, self.max_hlu)
        sp_state = self._set_permission(hlu, alu, self.host, sp_state)

        try:
            # Connect to iSCSI target
            path = self._connect_iscsi(hlu, alu)

            # Get glance api information.
            glance_servers = glance.get_api_servers()

            # Download the image from Glance.
            with utils.temporary_chown(path):
                self._execute('glance',
                              '--os-image-url',
                              'http://%s:%s/' % glance_servers.next(),
                              '--os-auth-token', str(ctxt.auth_token),
                              'image-download',
                              '--file', path,
                              str(image_id),
                              run_as_root=True)
        finally:
            # Disconnect to iSCSI target
            sp_state = self._disconnect_iscsi(hlu, sp_state)

    def copy_volume_to_image(self, ctxt, volume, image_service, image_id):
        actxt = self.admin_context
        sp_state = {'a': True, 'b': True}

        # Set an access premission to iSCSI target.
        volume = self.db.volume_get(actxt, volume['id'])
        self._ensure_emc_hlus(actxt, self.host)
        iscsi_properties = self._get_iscsi_properties(volume)
        alu = iscsi_properties['target_lun']
        hlu = self.db.emc_hlu_allocate(actxt, self.host, self.host, alu,
                                       self.min_hlu, self.max_hlu)
        sp_state = self._set_permission(hlu, alu, self.host, sp_state)

        try:
            # Connect to iSCSI target
            path = self._connect_iscsi(hlu, alu)

            # Get glance api information.
            glance_servers = glance.get_api_servers()

            # Upload the volume to Glance.
            with utils.temporary_chown(path):
                self._execute('glance',
                              '--os-image-url',
                              'http://%s:%s/' % glance_servers.next(),
                              '--os-auth-token', str(ctxt.auth_token),
                              'image-update',
                              '--file', path,
                              '--size', volume['size'] * 1024 ** 3,
                              str(image_id),
                              run_as_root=True)
        finally:
            # Disconnect to iSCSI target
            sp_state = self._disconnect_iscsi(hlu, sp_state)

    def prepare_purge_volume(self, context, volume_id):
        sp_state = {'a': True, 'b': True}
        vdisk = self.db.emc_alu_get_by_volume_id(context, volume_id)
        hlu = self.db.emc_hlu_get_by_alu_and_host(context,
                                                self.host,
                                                vdisk['alu'],
                                                self.host)
        if hlu:
            LOG.debug(_("Start deleting volume:hlu=%d alu=%d"),
                        hlu['hlu'], vdisk['alu'])
            sp_state = self._revoke_permission(hlu['hlu'], self.host, sp_state)

    def prepare_purge_snapshot(self, context, snapshot):
        sp_state = {'a': True, 'b': True}
        snapshot_name = self._build_snapshot_name(snapshot)
        args = ['storagegroup',
                '-list',
                '-gname', self.host]
        (out, sp_state) = self._run_nscli_wrap(args, sp_state)
        if snapshot_name in out:
            LOG.debug(_("Start deleting snapshot:gname=%s snapshotname=%s"),
                        self.host, snapshot_name)
            args = ['storagegroup', '-removesnapshot',
                    '-gname', self.host,
                    '-snapshotname', snapshot_name,
                    '-o']
            (out, sp_state) = self._run_nscli_wrap(args, sp_state,
                                                cmd_type='sg_remove_snapshot',
                                                gname=self.host,
                                                snapname=snapshot_name)

    def create_cloned_volume(self, volume, src_vref):
        actxt = self.admin_context
        sp_state = {'a': True, 'b': True}

        # Create LUN
        (d_alu, sp_state) = self._create_lun(
                                        volume['id'],
                                        volume['size'],
                                        sp_state)
        # Get Source Vlolume ALU
        src_volume = self.db.volume_get(actxt, src_vref['id'])
        iscsi_properties = self._get_iscsi_properties(src_volume)
        s_alu = iscsi_properties['target_lun']

        clone_group = self._add_clone(s_alu, d_alu)

        begin_time = datetime.now()
        time.sleep(FLAGS.emc_nscli_clone_sleep_time)

        # get clone id
        clone_id = self._get_clone_id(clone_group, d_alu)

        timeout_flag = False
        try:
            while True:
                if self._get_clone_state(clone_group, clone_id)\
                    == 'Synchronized':
                    break

                present_time = datetime.now()
                time_diff = present_time - begin_time
                if time_diff.total_seconds() > \
                    FLAGS.emc_nscli_clone_sync_timeout:
                    timeout_flag = True
                    raise exception.CinderException(_(
                                "Volume Clone Synchronization Timeout."))

                time.sleep(FLAGS.emc_nscli_clone_sleep_time)

        finally:
            # Fracture Lun
            args = ['snapview', '-fractureclone',
                    '-name', clone_group,
                    '-cloneid', clone_id, '-o']
            (out, sp_state) = self._run_nscli_wrap(args, sp_state,
                                            cmd_type='clone_fracture_clone',
                                            name=clone_group,
                                            cloneid=clone_id)

            if not timeout_flag and\
                (self._get_clone_state(clone_group, clone_id) != 'Consistent'\
                or self._get_clone_condition(clone_group, clone_id) !=\
                    'Administratively Fractured'):
                    raise exception.CinderException(_(
                                      "Fractureclone failed."))

            self._remove_clone(clone_group, clone_id)

        # set provider_location
        iscsi_portal = "%s:%d,%s" % (FLAGS.emc_nscli_service_ip,
                       FLAGS.emc_nscli_service_port,
                       FLAGS.emc_nscli_service_portal_group_tag)

        db_update = {}
        db_update['provider_location'] = ("%s %s %d" % (iscsi_portal,
                                          FLAGS.emc_nscli_target_iqn, d_alu))
        return db_update

    @lockutils.synchronized('emc-clone', 'cinder-')
    def _add_clone(self, s_alu, d_alu):
        # Check Clone Group
        sp_state = {'a': True, 'b': True}

        clone_group = "CG%s" % s_alu

        args = ['snapview', '-listclonegroup',
                '-Name', clone_group]
        (out, sp_state) = self._run_nscli_wrap(args, sp_state,
                                            cmd_type='clone_list_clonegroup',
                                            Name=clone_group,
                                            check_exit_code=[0, 69])

        # Create Clone Group
        create_clone_group_flag = True
        if clone_group in out:
            lines = out.splitlines()
            for index, line in enumerate(lines):
                if 'Name:' in line:
                    if line.split()[1] == clone_group:
                        create_clone_group_flag = False
                        break

        if create_clone_group_flag:
            args = ['snapview', '-createclonegroup',
                    '-name', clone_group,
                    '-luns', s_alu, '-o']
            (out, sp_state) = self._run_nscli_wrap(args, sp_state,
                                        cmd_type='clone_create_clonegroup',
                                        name=clone_group,
                                        luns=s_alu)

        # Add Clone Lun
        args = ['snapview', '-addclone',
                '-name', clone_group,
                '-luns', d_alu,
                '-SyncRate', FLAGS.emc_nscli_sync_rate]
        (out, sp_state) = self._run_nscli_wrap(args, sp_state,
                                        cmd_type='clone_add_clone',
                                        name=clone_group,
                                        luns=d_alu,
                                        syncrate=FLAGS.emc_nscli_sync_rate)

        return clone_group

    @lockutils.synchronized('emc-clone', 'cinder-')
    def _remove_clone(self, clone_group, clone_id):
        sp_state = {'a': True, 'b': True}

        # Remove Lun from Clone Group
        args = ['snapview', '-removeclone',
                '-name', clone_group,
                '-cloneid', clone_id, '-o']
        (out, sp_state) = self._run_nscli_wrap(args, sp_state,
                                            cmd_type='clone_remove_clone',
                                            name=clone_group,
                                            cloneid=clone_id)

        # destroy Clone Group
        args = ['snapview', '-listclone',
                '-Name', clone_group, '-CloneLUNs']
        (out, sp_state) = self._run_nscli_wrap(args, sp_state,
                                                cmd_type='clone_list_clone',
                                                Name=clone_group)

        if out.isspace():
            args = ['snapview', '-destroyclonegroup',
                    '-name', clone_group, '-o']
            (out, sp_state) = self._run_nscli_wrap(args, sp_state,
                                        cmd_type='clone_destroy_clonegroup',
                                        name=clone_group)

    def _get_clone_id(self, clone_group, d_alu):
        sp_state = {'a': True, 'b': True}

        args = ['snapview', '-listclone',
                '-Name', clone_group, '-CloneLUNs']
        (out, sp_state) = self._run_nscli_wrap(args, sp_state,
                                                cmd_type='clone_list_clone',
                                                Name=clone_group)

        lines = out.splitlines()
        for index, line in enumerate(lines):
            if 'CloneLUNs:' in line and line.split()[1] == str(d_alu):
                return lines[index-1].split()[1]

        raise exception.CinderException(_("Clone ID is not found."))

    def _get_clone_state(self, clone_group, clone_id):
        sp_state = {'a': True, 'b': True}

        args = ['snapview', '-listclone',
                '-Name', clone_group, '-cloneid', clone_id,
                '-CloneState']
        (out, sp_state) = self._run_nscli_wrap(args, sp_state,
                                                cmd_type='clone_list_clone',
                                                Name=clone_group,
                                                cloneid=clone_id,
                                                check_exit_code=[0, 70])

        lines = out.splitlines()
        for index, line in enumerate(lines):
            if 'CloneID:' in line:
                return lines[index+1].split()[1]

        raise exception.CinderException(_("Clone state is not found."))

    def _get_clone_condition(self, clone_group, clone_id):
        sp_state = {'a': True, 'b': True}

        args = ['snapview', '-listclone',
                '-Name', clone_group, '-cloneid', clone_id,
                '-CloneCondition']
        (out, sp_state) = self._run_nscli_wrap(args, sp_state,
                                                cmd_type='clone_list_clone',
                                                Name=clone_group,
                                                cloneid=clone_id,
                                                check_exit_code=[0, 70])

        lines = out.splitlines()
        for index, line in enumerate(lines):
            if 'CloneID:' in line:
                return lines[index+1].split(None, 1)[1]

        raise exception.CinderException(_("Clone condition is not found."))
