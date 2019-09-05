#!/usr/bin/python
# -*- coding: utf-8 -*-

import datetime
import json
import os
import shutil
import subprocess


class Backup(object):

    def __init__(self):
        config_file = "/var/lib/config-data/mysql/etc" \
            "/puppet/hieradata/service_configs.json"
        with open(config_file, 'r') as f:
            load_dict = json.load(f)
        self.pass_word = load_dict['mysql::server::root_password']

        # databases backup path
        databases_backup_path = "/var/tmp/mysql_backup/"
        if os.path.exists(databases_backup_path):
            shutil.rmtree(databases_backup_path)
        os.makedirs(databases_backup_path, mode=0o777)

        # filesystem backup path
        filesystem_backup_path = "/var/tmp/filesystem_backup/"
        if os.path.exists(filesystem_backup_path):
            shutil.rmtree(filesystem_backup_path)
        os.makedirs(filesystem_backup_path, mode=0o777)

    def backup_databases(self):

        mysql_command = 'select distinct table_schema ' \
                        'from information_schema.tables ' \
                        'where engine=\'innodb\' and ' \
                        'table_schema != \'mysql\';'
        code, stdout, stderr = run_command(
            'mysql -u root -p{0} -e "{1}" -s -N '.format(
                self.pass_word, mysql_command))
        for db in stdout.split():
            dump_r_code, dump_stdout, dump_stderr = run_command(
                'mysqldump -uroot -p{0} --single-transaction --databases {1}'
                .format(self.pass_word, db))
            filename = '/var/tmp/mysql_backup/openstack_databases-' + \
                db + '-' + datetime.datetime.now().strftime(
                    '%Y-%m-%d-%H:%M:%S') + '.sql'
            with open(filename, 'w') as f:
                f.write(dump_stdout)

    def backup_databases_grants(self):

        mysql_command = \
            'select concat(\'\\"show grants for \'\'\', ' \
            'user,\'\'\'@\'\'\',host,\'\'\';\\"\') ' \
            'from mysql.user ' \
            'where (length(user) > 0 and user NOT LIKE \'root\')'
        code, statements, stderr = run_command(
            'mysql -u root -p{0} -e "{1}" -s -N'.format(
                self.pass_word, mysql_command))

        grants = None
        for statement in statements.splitlines():
            code, grant, stderr = run_command(
                'mysql -u root -p{0} -s -N -e {1}'.format(
                    self.pass_word, statement))
            if grants:
                grants += grant
            else:
                grants = grant
        grants = grants.replace('\n', ';\n')[:-1]
        filename = '/var/tmp/mysql_backup/openstack_databases_grants-' + \
            datetime.datetime.now().strftime('%Y-%m-%d-%H:%M:%S') + '.sql'
        with open(filename, 'w') as f:
            f.write(grants)

    def filesystem_backup(self):
        files = \
            '/var/lib/config-data/puppet-generated/cinder/etc/cinder \\' \
            '/etc/corosync/ \\' \
            '/var/lib/config-data/puppet-generated/glance_api/etc/glance/ \\' \
            '/etc/haproxy \\' \
            '/var/lib/config-data/puppet-generated/heat/etc/heat/ \\' \
            '/etc/httpd/ \\' \
            '/var/lib/config-data/puppet-generated/keystone/etc/keystone \\' \
            '/var/lib/config-data/puppet-generated/neutron/etc/neutron \\' \
            '/var/lib/config-data/puppet-generated/nova/etc/nova \\' \
            '/etc/openvswitch/ \\' \
            '/var/lib/config-data/puppet-generated/rabbitmq/etc/rabbitmq \\' \
            '/var/lib/config-data/puppet-generated/memcached/etc/' \
            'sysconfig/memcached \\' \
            '/var/lib/heat-cfntools \\' \
            '/var/lib/heat-config \\' \
            '/var/log/containers'
        date = datetime.datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
        target_file = '/var/tmp/filesystem_backup/fs_backup-date-'\
            + date + '.tar.gz'
        cmd = 'tar --xattrs --ignore-failed-read -zcvf ' \
            + target_file + ' ' + files
        run_command(cmd)


def run_command(cmd):
    assert isinstance(cmd, str), 'Expect command to be a string'
    assert cmd and len(cmd), 'Empty command provided'

    try:
        proc = subprocess.Popen(cmd,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                shell=True,
                                close_fds=(os.name != 'nt'))
        stdout, stderr = proc.communicate()
        ret_code = proc.returncode
    except OSError as run_ex:
        raise run_ex

    return ret_code, str(stdout), str(stderr)


if __name__ == '__main__':
    backup = Backup()
    backup.filesystem_backup()
    backup.backup_databases()
    backup.backup_databases_grants()
    tar_file = '/var/tmp/mysql_backup/openstack_databases_backup.tar.gz'
    run_command(
        'tar -zcvf ' + tar_file + ' ' + '/var/tmp/mysql_backup/')
