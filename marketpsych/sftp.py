#!/usr/bin/env python3
import argparse
import re
import sys
from calendar import monthrange
from datetime import date, datetime, time
from enum import Enum
from functools import partial, reduce
from pathlib import Path
from shutil import copyfileobj
from zipfile import ZipFile
import logging
from dataclasses import dataclass
import typing as T
import io
import paramiko
from itertools import islice
import base64
import os

logger = logging.getLogger(__name__)

ASSET_CLASSES = "CMPNY CMPNY_AMER CMPNY_APAC CMPNY_EMEA CMPNY_ESG CMPNY_GRP COM_AGR COM_ENM COU COU_ESG COU_MKT CRYPTO CUR".split()
FREQUENCIES = "W365_UDAI WDAI_UDAI WDAI_UHOU W01M_U01M".split()
BUCKETS = "monthly daily minutely".split()
AssetClass = Enum("AssetClass", {ac: ac for ac in ASSET_CLASSES})
Frequency = Enum("Frequency", {frq: frq for frq in FREQUENCIES})
Bucket = Enum("Bucket", {grp: grp for grp in BUCKETS})
DATE_PAT = re.compile(
    r"""(\d\d\d\d)                                    # yyyy
    (?: \W? (\d\d)                                    # mm
        (?: \W? (\d\d)                                # dd
            (?: \W? (\d\d) \W? (\d\d) (?:\W.*)? )?    # HH, MM and leftovers
        )?
    )?
    $""",
    re.VERBOSE,
)
DATETIME_FMT = "yyyy(-?mm(-?dd(-?HHMM)?)?)?"
SSH_DIR = Path.home() / ".ssh"

TEMPLATES = {
    Frequency: "{frequency}/{bucket}",
    AssetClass: "{asset_class}/{frequency}/{bucket}",
    Bucket: "{bucket}",
}
DEFAULT_TEMPLATE = "{prefix}/{asset_class}/{frequency}/{bucket}"
DEFAULT_PREFIX = Path("/mrn-mi-w/PRO/MI4")
DEFAULT_HOST = "sftp.news.refinitiv.com"
DATAFRAME_STR = "pandas://"
LS_STR = "ls://"

Period = T.Tuple[datetime, datetime]


def decompress(obj, fp, func):
    """Decompress file-like object obj with filepath fp, and run func on it"""
    if fp.suffix == ".zip":
        with ZipFile(obj) as zf:
            try:
                [info] = zf.infolist()
            except ValueError:
                raise Exception("Must be exactly 1 entry in zip archive: {path}")
            with zf.open(info) as inner:
                # return func(io.TextIOWrapper(obj))
                return func(inner, info.filename)
    return func(obj, fp.name)


def parse_date(s: str, end=False) -> datetime:
    match = DATE_PAT.match(s)
    if not match:
        raise ValueError(f"Can't parse timestamp: {repr(s)}. Expecting format: {DATETIME_FMT}")
    defts = (0, 12, 31, 23, 59) if end else (0, 1, 1, 0, 0)
    year, month, day, hour, minute = (int(part or d) for part, d in zip(match.groups(), defts))
    return datetime(year, month, min(day, monthrange(year, month)[1]), hour, minute)


def parse_period(start, end=None):
    return parse_date(start), parse_date(end or start, end=True)


def overlaps(period1, period2):
    """Do periods intersect?"""
    return max(period1[0], period2[0]) <= min(period1[1], period2[1])


def parse_file_period(filename):
    """Parse time period of RMA filename"""
    return parse_period(filename.split(".")[4])


class Output:
    """Object which determines how to copy input files into output file or directory"""

    @property
    def result(self):
        pass

    def copy(self, in_, filename):
        pass

    def copy_file(self, sftp, fp, attr):
        with sftp.open(str(fp)) as in_:
            in_.prefetch(attr.st_size)  # like in getfo() implementation
            return decompress(in_, fp, self.copy)

    @staticmethod
    def parse(path: str) -> "Output":
        """
        :param path: Possible options:
        - "pandas://" to read remote files into dataframe and return it.
        - "ls://" to list remote files without downloading
        - "" (empty string) to concatenate into stdout
        - FILEPATH to concatenate remote files into single file on disk.
        - DIR/ or '.' to download files one-by-one into directory.
        """
        if not path:
            return FileOutput(sys.stdout.buffer)
        if path == DATAFRAME_STR:
            return DataFrameOutput()
        if path == LS_STR:
            return MockOutput()
        logger.debug(f"Creating parent dir for {path}")
        Path(path).parent.mkdir(exist_ok=True, parents=True)
        if path.endswith("/") or path in (".", ".."):
            Path(path).mkdir(exist_ok=True)
            return DirOutput(Path(path))
        return FileOutput(open(path, "wb"))  # TODO move this inside `with` clause


@dataclass(frozen=True)
class MockOutput(Output):
    """List input files and their attributes, don't copy anything"""

    def copy_file(self, _, fp, attr):
        print(attr)


@dataclass
class FileOutput(Output):
    """Concatenate all input files into output file"""

    out: T.BinaryIO
    header: T.Optional[str] = None

    def copy(self, in_, filename):
        write = self.out.write
        read = in_.read
        header, buf = read(32768).split(b"\n", 1)
        if not self.header:
            write(header + b"\n")
            self.header = header
        elif self.header != header:
            raise Exception(f"Header mismatch in {in_}.\nLast: {self.header!r}\nThis: {header!r}")
        while buf:
            write(buf)
            buf = read(32768)


@dataclass
class DataFrameOutput(Output):
    """Read files into pandas DataFrame"""

    df: T.Any = None  # should be pd.DataFrame, but don't want to import pandas yet

    @property
    def result(self):
        return self.df

    def copy_file(self, sftp, fp, attr):
        import pandas as pd

        bs = io.BytesIO()
        sftp.getfo(str(fp), bs)
        df: pd.DataFrame = pd.read_csv(
            bs, sep="\t", na_values="", compression="zip" if fp.suffix == ".zip" else None
        )  # type:ignore
        logger.debug(f"{type(self)}: Appending {len(df)} records")
        self.df = df if self.df is None else self.df.append(df, ignore_index=True)


@dataclass(frozen=True)
class DirOutput(Output):
    """Copy files into output directory"""

    out: Path

    def copy_file(self, sftp, fp, attr):
        sftp.get(str(fp), str(self.out / fp.name))


class SFTPClient(paramiko.SFTPClient):
    def iter_dirs(
        self,
        asset_class: AssetClass,
        frequency: Frequency,
        buckets: T.Tuple[Bucket, ...] = (),
        template: str = DEFAULT_TEMPLATE,
        prefix: Path = DEFAULT_PREFIX,
    ) -> T.Iterable[Path]:
        """Generate directories where the files can be located on SFTP

        :param buckets: Restrict to these time buckets. If empty, will use all: monthly, daily, minutely
        """
        template = template or self.detect_template()
        for bucket in buckets or Bucket:
            dir = template.format(
                prefix=str(prefix),
                asset_class=asset_class.name + ("_COR" if frequency is Frequency.W365_UDAI else ""),
                bucket=bucket.name,
                frequency=frequency.name,
            )
            yield Path(dir)

    def copy_files(self, dir: Path, period: Period, output: Output) -> int:
        """Copy remote files to output and return number of files copied"""
        try:
            listing = self.listdir_attr(str(dir))
        except FileNotFoundError as e:
            logger.warning(f"{dir}: {e}")
            return 0
        logger.debug(f"Searching in {dir}")
        matching = [attr for attr in listing if overlaps(period, parse_file_period(attr.filename))]
        logger.info(f"Found {len(matching)} files in {dir}")
        for attr in matching:
            fp = dir / attr.filename
            logger.info(f"Getting {fp}")
            output.copy_file(self, fp, attr)
        return len(matching)

    def copy_files_in_dirs(self, dirs: T.Iterable[Path], period: Period, output: Output):
        n_files = sum(self.copy_files(dir, period=period, output=output) for dir in dirs)
        if n_files == 0:
            logger.warning("No files found within time range")
        else:
            logger.debug(f"Processed {n_files} files")
        return output.result

    def download(
        self,
        asset_class: AssetClass,
        frequency: Frequency,
        start: datetime,
        end: datetime,
        output: T.Union[Output, str] = DataFrameOutput(),
        buckets: T.Tuple[Bucket, ...] = (),
        prefix: Path = DEFAULT_PREFIX,
        trial: bool = False,
        template: str = DEFAULT_TEMPLATE,
    ):
        """
        Download files from SFTP and either read them into dataframe or write to disk

        :param asset_class: asset class, e.g. AssetClass.CMPNY
        :param frequency: window length and update frequency, e.g. Frequency.W01M_U01M
        :param start: period start
        :param end: period end
        :param output: Output instance, default is DataFrameOutput
        :param buckets: Restrict search to given directories (daily / minutely / hourly).
        If empty (default), then search in all directories.
        :param prefix: Root folder on remote side
        :param trial: if true, append TRIAL to prefix
        :param template: Template for directory structure.
        If empty, template will be detected based on directory listing.
        If not empty, these variables will be substituted: prefix, asset_class, frequency, bucket.
        :returns: dataframe if output is DataFrameOutput
        """
        dirs = self.iter_dirs(
            asset_class=asset_class,
            frequency=frequency,
            buckets=buckets,
            template=template,
            prefix=prefix / ("TRIAL" if trial else ""),
        )
        return self.copy_files_in_dirs(
            dirs,
            period=(start, end),
            output=output if isinstance(output, Output) else Output.parse(output),
        )

    def detect_template(self) -> str:
        try:
            dir, *dirs = self.listdir()
        except ValueError:
            raise paramiko.SFTPError("Empty root folder")
        for ty, template in TEMPLATES.items():
            try:
                _ = ty(dir)
                return template
            except ValueError:
                continue
        raise paramiko.SFTPError("Can't detect directory structure")


def putty_key_messages(lines: T.TextIO) -> T.Iterable[paramiko.Message]:
    for line in lines:
        m = re.search(r"-Lines:\s*(\d+)", line)
        if m:
            yield paramiko.Message(
                base64.standard_b64decode("".join(s.strip() for s in islice(lines, int(m[1]))))
            )


def load_private_key(key: T.Union[Path, T.TextIO]):
    with open(key) if isinstance(key, (str, Path)) else key as f:
        key_str = f.read()
    try:
        return paramiko.RSAKey.from_private_key(io.StringIO(key_str))
    except paramiko.SSHException:  # not OpenSSH format. Try parsing as Putty key
        pub, pvt = putty_key_messages(io.StringIO(key_str))
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.hazmat.backends import default_backend

        d, p, q, iqmp = pvt.get_mpint(), pvt.get_mpint(), pvt.get_mpint(), pvt.get_mpint()
        pvt_key = rsa.RSAPrivateNumbers(
            p=p,
            q=q,
            d=d,
            iqmp=iqmp,
            dmp1=rsa.rsa_crt_dmp1(d, p),
            dmq1=rsa.rsa_crt_dmq1(d, q),
            public_numbers=paramiko.RSAKey(pub).public_numbers,
        ).private_key(backend=default_backend())
        return paramiko.RSAKey(key=pvt_key)


def connect(
    user: str, key: T.Union[None, Path, T.TextIO] = None, host: str = DEFAULT_HOST
) -> SFTPClient:
    """
    Connect to host using credentials and return SFTPClient
    :param user: User ID as string
    :param key: Private key file object or filepath. If None, will use SSH_DIR/user
    :param host: SFTP server hostname
    """
    transport = paramiko.Transport(host)
    transport.connect(username=user, pkey=load_private_key(key or SSH_DIR / user))
    return SFTPClient.from_transport(transport)  # type: ignore


class Args(argparse.Namespace):
    def loglevel(self):
        return max(logging.WARNING + 10 * (self.quiet - self.verbose), logging.DEBUG)

    def parse_period(self):
        try:
            start, end = parse_period(self.start, self.end)
            logger.debug(f"Time range: {start} to {end}")
            return (start, end)
        except ValueError as e:
            parser.error(str(e))
            exit()


class ArgumentParser(argparse.ArgumentParser):
    def add_arg(self, name, *args, **kwargs):
        self.add_argument(name, *args, metavar=name.lstrip("-").upper(), **kwargs)

    def count_opt(self, *args, **kwargs):
        self.add_argument(*args, action="count", default=0, **kwargs)

    def enum_arg(self, cls, name, *args, **kwargs):
        help = "{{{}}}\n{}".format(",".join(x.name for x in cls), kwargs.pop("help", ""))
        self.add_arg(name, *args, type=cls, choices=cls, help=help, **kwargs)

    def date_arg(self, *args, **kwargs):
        self.add_arg(*args, help=f"Date or datetime: {DATETIME_FMT}", nargs="?", **kwargs)


def cli_parser() -> ArgumentParser:
    cli = ArgumentParser(
        description="Download RMA files",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    cli.add_argument(
        "--key",
        "-k",
        help=f"""Private key file location.
        Key must be in OpenSSH format (not Putty) and unencrypted.
        If not provided, will try {SSH_DIR}/<user>""",
    )
    cli.add_arg("user", help="Username (account number)")
    cli.enum_arg(AssetClass, "asset_class")
    cli.enum_arg(Frequency, "frequency")
    cli.date_arg("start", default=str(date.today()))
    cli.date_arg("end")
    cli.enum_arg(Bucket, "--buckets", "-b", action="append", help="Restrict to these time buckets")
    cli.add_argument("--host", default=DEFAULT_HOST, help="Hostname of SFTP server")
    cli.count_opt("--verbose", "-v", help="More verbose output (also try -vv)")
    cli.count_opt("--quiet", "-q", help="Less verbose output (also try -qq)")
    cli.add_argument(
        "--output",
        "-o",
        type=str if os.environ.get("SFTP_TEST") else Output.parse,
        help=Output.parse.__doc__,
        metavar="FILE|DIR/",
    )
    cli.add_argument("--prefix", help="Directory prefix", default=DEFAULT_PREFIX, type=Path)
    cli.add_argument("--template", help="Directory structure", default=DEFAULT_TEMPLATE)
    cli.add_argument("--trial", action="store_true")
    return cli


if __name__ == "__main__":
    parser = cli_parser()
    args = parser.parse_args(args=None, namespace=Args())
    logger.setLevel(args.loglevel())
    logger.addHandler(logging.StreamHandler())
    logger.debug(f"Log level: {logger.getEffectiveLevel()}.\nCLI args: {args!r}")
    start, end = args.parse_period()
    with connect(user=args.user, key=args.key, host=args.host) as sftp:
        result = sftp.download(
            asset_class=args.asset_class,
            frequency=args.frequency,
            buckets=args.buckets,
            template=args.template,
            prefix=args.prefix,
            trial=args.trial,
            output=args.output,
            start=start,
            end=end,
        )
        if result is not None:
            print(result)

