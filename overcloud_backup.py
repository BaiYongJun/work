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
        backup_path = "/var/tmp/mysql_backup/"
        if os.path.exists(backup_path):
            shutil.rmtree(backup_path)
        os.makedirs(backup_path, mode=0755)

    def backup_databases(self):

        mysql_command = 'select distinct table_schema ' \
                        'from information_schema.tables ' \
                        'where engine=\'innodb\' and ' \
                        'table_schema != \'mysql\';'
        code, stdout, stderr = self.run_command(
            'mysql -u root -p{0} -e "{1}" -s -N '.format(
                self.pass_word, mysql_command))
        for db in stdout.split():
            dump_r_code, dump_stdout, dump_stderr = self.run_command(
                'mysqldump -uroot -p{0} --single-transaction --databases {1}'
                .format(self.pass_word, db))
            filename = '/var/tmp/mysql_backup/openstack_databases-' + \
                db + '-' + datetime.datetime.now().strftime('%Y-%m-%d-%H:%M:%S') + '.sql'
            with open(filename, 'w') as f:
                f.write(dump_stdout)

    def backup_databases_grants(self):

        mysql_command = \
            'select concat(\'\\"show grants for \'\'\',user,\'\'\'@\'\'\',host,\'\'\';\\"\') ' \
            'from mysql.user ' \
            'where (length(user) > 0 and user NOT LIKE \'root\')'
        code, statements, stderr = self.run_command(
            'mysql -u root -p{0} -e "{1}" -s -N'.format(
                self.pass_word, mysql_command))

        grants = None
        for statement in statements.splitlines():
            code, grant, stderr = self.run_command(
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

    def run_command(self, cmd):
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
    backup.backup_databases()
    backup.backup_databases_grants()
    tar_file = '/var/tmp/mysql_backup/openstack_databases_backup.tar.gz'
    backup.run_command(
        'tar -zcvf ' + tar_file + '/var/tmp/mysql_backup/')
