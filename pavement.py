from paver.easy import *
from paver import doctools
from paver.setuputils import setup

options(
        sphinx=Bunch(builddir=".build"),
)

def sphinx_builddir(options):
    return path("docs") / options.sphinx.builddir / "html"


@task
def clean_docs(options):
    sphinx_builddir(options).rmtree()


@task
@needs("clean_docs", "paver.doctools.html")
def html(options):
    destdir = path("Documentation")
    destdir.rmtree()
    builtdocs = sphinx_builddir(options)
    builtdocs.move(destdir)


@task
@needs("clean_docs", "paver.doctools.html")
def ghdocs(options):
    builtdocs = sphinx_builddir(options)
    sh("sphinx-to-github", cwd=builtdocs)
    sh("git checkout gh-pages && \
            cp -r %s/* .    && \
            git commit . -m 'Rendered documentation for Github Pages.' && \
            git push origin gh-pages && \
            git checkout master" % builtdocs)


@task
@needs("clean_docs", "paver.doctools.html")
def upload_pypi_docs(options):
    builtdocs = path("docs") / options.builddir / "html"
    sh("python setup.py upload_sphinx --upload-dir='%s'" % (builtdocs))


@task
@needs("upload_pypi_docs", "ghdocs")
def upload_docs(options):
    pass


@task
def flakes(options):
    sh("find redish -name '*.py' | xargs pyflakes")


@task
def clean_readme(options):
    path("README").unlink()
    path("README.rst").unlink()


@task
@needs("clean_readme")
def readme(options):
    sh("python contrib/release/sphinx-to-rst.py docs/templates/readme.txt \
            > README.rst")
    sh("ln -sf README.rst README")


@task
def bump(options):
    sh("bump -c redish")


@task
@cmdopts([
    ("coverage", "c", "Enable coverage"),
    ("quick", "q", "Quick test"),
    ("verbose", "V", "Make more noise"),
])
def test(options):
    cmd = "python manage.py test"
    if getattr(options, "coverage", False):
        cmd += " --coverage"
    if getattr(options, "quick", False):
        cmd = "env QUICKTEST=1 %s" % cmd
    if getattr(options, "verbose", False):
        cmd += " --verbosity=2"
    sh(cmd, cwd="tests")


@task
@cmdopts([
    ("noerror", "E", "Ignore errors"),
])
def pep8(options):
    noerror = getattr(options, "noerror", False)
    return sh("""find . -name "*.py" | xargs pep8 | perl -nle'\
            print; $a=1 if $_}{exit($a)'""", ignore_error=noerror)


@task
def removepyc(options):
    sh("find . -name '*.pyc' | xargs rm")


@task
@needs("removepyc")
def gitclean(options):
    sh("git clean -xdn")


@task
@needs("removepyc")
def gitcleanforce(options):
    sh("git clean -xdf")


@task
@needs("pep8", "test", "gitclean")
def releaseok(options):
    pass


@task
@needs("releaseok", "removepyc", "upload_docs")
def release(options):
    pass
