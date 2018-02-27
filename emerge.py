import dnf
import dnf.cli
from glob import glob
import logging
import threading
import tempfile
import subprocess
import shutil
import os

logger = logging.getLogger('dnf')


class ErrorThread(threading.Thread):
    _my_exception = None

    def run(self, *args):
        try:
            self._run(*self._args)
        except Exception as ex:
            self._my_exception = ex


class BuildThread(ErrorThread):
    @property
    def branch(self):
        return 'master'

    @property
    def template_mock_config(self):
        return '/etc/mock/fedora-rawhide-x86_64.cfg'

    def _run(self, workdir, pkg):
        pkgdir = os.path.join(workdir, pkg)

        # Grab sources
        logger.info('Grabbing sources')
        subprocess.run(['fedpkg', 'clone', '--anonymous', '--branch', self.branch, 'rpms/%s' % pkg, pkgdir],
                       check=True)

        # Generate mockconfig
        logger.info('Generating mock config')
        mock_config = os.path.join(workdir, '_mockconfig', 'emerge-%s.cfg' % pkg)
        with open(self.template_mock_config, 'r') as template:
            with open(mock_config, 'w') as out:
                out.write("config_opts['basedir'] = '%s'\n" % (os.path.join(workdir, '_mockroots')))
                for line in template.readlines():
                    if "config_opts['root']" in line:
                        out.write("config_opts['root'] = 'emerge-%s'\n" % pkg)
                    else:
                        out.write(line)

        # Run mockbuild
        logger.info('Building')
        subprocess.run(['fedpkg', 'mockbuild', '--root', mock_config, '--no-clean-all'], check=True, cwd=pkgdir)


@dnf.plugin.register_command
class EmergeCommand(dnf.cli.Command):
    aliases = ['emerge']

    workdir = None

    def configure(self):
        self.cli.demands.available_repos = True
        self.cli.demands.sack_activation = True
        self.cli.demands.root_user = True
        self.cli.demands.resolving = True

    @staticmethod
    def set_argparser(parser):
        parser.add_argument('package', nargs='+', metavar='package',
                            help='Package to emerge')
        parser.add_argument('--workdir')
        parser.add_argument('--skip-build', action='store_true')
        parser.add_argument('--skip-clean', action='store_true')

    def run_transaction(self):
        self._rmworkdir()

    def _rmworkdir(self):
        if self.workdir and not self.opts.workdir and not self.opts.skip_clean:
            shutil.rmtree(self.workdir)

    def run(self):
        try:
            self._run()
        except:
            self._rmworkdir()
            raise

    def _run(self):
        q = self.base.sack.query()
        pkgs = self.base.sack.query().available().filter(name=self.opts.package).latest().run()
        if not pkgs:
            raise dnf.exceptions.Error('no package matched')

        to_build_install = {}
        for pkg in pkgs:
            if pkg.source_name in to_build_install:
                to_build_install[pkg.source_name].add(pkg.name)
            else:
                to_build_install[pkg.source_name] = set([pkg.name])

        logger.info('Building/installing: %s' % to_build_install)

        if self.opts.workdir:
            self.workdir = self.opts.workdir
        else:
            self.workdir = tempfile.TemporaryDirectory(prefix='dnf-emerge-').name

        logger.debug('Workdir: %s', self.workdir)

        self._build(self.workdir, to_build_install)
        pkgs = self._find_packages(self.workdir, to_build_install)

        err_pkgs = []
        for pkg in self.base.add_remote_rpms(pkgs):
            try:
                self.base.package_install(pkg)
            except dnf.exceptions.MarkingError:
                logger.info('Unable to install %s' % self.base.output.term.bold(pkg.location))
                err_pkgs.append(pkg)
        
        if len(err_pkgs) != 0 and strict:
            raise dnf.exceptions.PackagesNotAvailableError(
                'Unable to find a match', packages=err_pkgs)

    @staticmethod
    def _is_wanted_file(fname, haystack):
        for needle in haystack:
            if fname.endswith('.src.rpm'):
                continue
            if not fname.startswith(needle + '-'):
                continue
            rest = fname[len(needle)+1:].split('-')
            if len(rest) > 2:
                continue
            if not rest[0][0].isdigit():
                continue
            return True
        return False

    def _find_packages(self, workdir, to_build_install):
        to_install = []

        for source, binaries in to_build_install.items():
            sourcedir = os.path.join(workdir, source, 'results_%s' % source, '*', '*', '*.rpm')
            for fpath in glob(sourcedir):
                fname = os.path.basename(fpath)
                if self._is_wanted_file(fname, binaries):
                    to_install.append(fpath)
        
        logger.info('Marking for installation: %s', to_install)
        return to_install

    def _build(self, workdir, to_build_install):
        if self.opts.skip_build:
            logger.error('Skipping build per request')
            return

        os.makedirs(os.path.join(workdir, '_mockconfig'))
        os.makedirs(os.path.join(workdir, '_mockroots'))

        buildthreads = []
        for pkg in to_build_install.keys():
            bthread = BuildThread(name='emerge-build-%s' % pkg, args=(workdir, pkg))
            buildthreads.append(bthread)
            bthread.start()

        logger.info('All builds started, waiting for them to finish...')

        for bthread in buildthreads:
            bthread.join()
            if bthread._my_exception:
                raise bthread._my_exception

        logger.info('All builds finished')