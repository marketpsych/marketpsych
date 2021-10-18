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
from dataclasses import dataclass, replace
import typing as T
import io
import paramiko
from itertools import islice
import base64
import os
import abc
import zipfile

logger = logging.getLogger(__name__)

ASSET_CLASSES = "CMPNY CMPNY_AMER CMPNY_APAC CMPNY_EMEA CMPNY_ESG CMPNY_GRP COM_AGR COM_ENM COU COU_ESG COU_MKT CRYPTO CUR".split()
FREQUENCIES = "W365_UDAI WDAI_UDAI WDAI_UHOU W01M_U01M".split()
BUCKETS = "monthly daily hourly minutely".split()
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
DEFAULT_CACHE = Path("marketpsych_files")

DATAFRAME_STR = "pandas://"
LS_STR = "ls://"

Period = T.Tuple[datetime, datetime]


def decompress(obj, fp, func):
    """Decompress file-like object obj with filepath fp, and run func on it"""
    fp = Path(fp)
    if fp.suffix == ".zip":
        with ZipFile(obj) as zf:
            try:
                [info] = zf.infolist()
            except ValueError:
                raise Exception("Must be exactly 1 entry in zip archive: {path}")
            with zf.open(info) as inner:
                # return func(io.TextIOWrapper(obj))
                return func(io.TextIOWrapper(inner), Path(info.filename))
    return func(io.TextIOWrapper(obj), fp.name)


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


def periods_union(periods: T.Iterable[T.Optional[Period]]):
    try:
        starts, ends = zip(*(per for per in periods if per))
        return min(starts), max(ends)
    except ValueError:
        return None


def is_subperiod(period1: Period, period2: T.Optional[Period]):
    """Is period1 inside period2?"""
    return period2 and period2[0] <= period1[0] and period1[1] <= period2[1]


def parse_file_period(filename):
    """Parse time period of RMA filename"""
    return parse_period(str(filename).split(".")[4])


def sftp_dir(
    asset_class: AssetClass,
    frequency: Frequency,
    bucket: Bucket,
    prefix=DEFAULT_PREFIX,
    template: str = DEFAULT_TEMPLATE,
) -> Path:
    """SFTP directory for asset_class, frequency, bucket"""
    return Path(
        template.format(
            prefix=str(prefix),
            asset_class=asset_class.name + ("_COR" if frequency is Frequency.W365_UDAI else ""),
            bucket=bucket.name,
            frequency=frequency.name,
        )
    )


class Output:
    """Object which determines how to copy input files into output file or directory"""

    @property
    def result(self):
        pass

    @abc.abstractmethod
    def copy_file(self, sftp: "SFTPClient", fp: Path, attr=None, accum=None):
        pass

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

    def copy_file(self, _, fp, attr=None, accum=None):
        print(attr)


@dataclass
class FileOutput(Output):
    """Concatenate all input files into output file"""

    out: T.BinaryIO
    header: T.Optional[str] = None

    def copy_file(self, sftp, fp, attr=None, accum=None):
        return sftp.decompress(fp, self.copy)

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


TSV_FIELD = r"[^\t]*"


def choice_re(vals):
    return f'(?:{"|".join(re.escape(s) for s in vals)})' if vals else None


def line_re(header, **kwargs):
    fields = []
    for name, val in kwargs.items():
        if val is not None:
            ix = header.index(name)
            fields += [TSV_FIELD] * (1 + ix - len(fields))
            fields[ix] = val
    pat = re.compile("\t".join(fields + [""]))
    logger.debug(f"Line regex: {pat.pattern!r}")
    return pat


@dataclass(frozen=True)
class DataFrameOutput(Output):
    """Read files into pandas DataFrame"""

    assets: T.Tuple[str, ...] = ()
    sources: T.Tuple[str, ...] = ()
    start: datetime = datetime.min
    end: datetime = datetime.max
    read_csv_opts: T.Tuple[T.Tuple[str, T.Any], ...] = ()

    def pattern(self, header, capture_date=False):
        return line_re(
            header,
            assetCode=choice_re(self.assets),
            dataType=choice_re(self.sources),
            windowTimestamp=f"({TSV_FIELD})" if capture_date else None,
        )

    def filter_assets_dates(self, lines, header):
        """Filter by asset (if needed) and windowTimestamp"""

    def filter_rows(self, fr, path):
        start, end = parse_file_period(path)
        keep_all_dates = self.start <= start and end <= self.end
        if not self.assets and not self.sources and keep_all_dates:  # no filtering
            return fr
        buf = io.StringIO()
        header = next(fr)
        buf.write(header)
        pat = self.pattern(header.split("\t"), capture_date=not keep_all_dates)
        if keep_all_dates:
            lines = filter(pat.match, fr)
        else:
            START, END = (t.strftime("%FT%T.000Z") for t in (self.start, self.end))
            lines = (line for line in fr if (lambda m: m and START <= m[1] <= END)(pat.match(line)))
        for line in lines:
            buf.write(line)
        buf.seek(0)
        return buf

    def read_tsv(self, fr, path):
        import pandas as pd

        read_tsv = partial(pd.read_csv, sep="\t", na_values="", **dict(self.read_csv_opts))
        df: pd.DataFrame = read_tsv(self.filter_rows(fr, path))  # type:ignore
        logger.debug(f"{type(self)}: Loaded {len(df)} records")
        if "windowTimestamp" in df.columns:
            df.windowTimestamp = pd.to_datetime(df.windowTimestamp)
        return df

    def copy_file(self, sftp, fp, attr=None, accum=None):
        with sftp.open(fp, "rb") as fr:
            df = decompress(fr, fp, self.read_tsv)  # type: ignore
            return df if accum is None else accum.append(df, ignore_index=True)


@dataclass(frozen=True)
class DirOutput(Output):
    """Copy files into output directory"""

    out: Path

    def copy_file(self, sftp, fp, attr=None, accum=None):
        sftp.get_file(fp, self.out)  # str(fp), str(self.out / fp.name))


class CachingSFTPClient(paramiko.SFTPClient):
    cache: Path

    def cached_path(self, path: Path):
        return self.cache / (path.relative_to("/") if path.is_absolute() else path)

    def ensure_cache(self, path: Path):
        cached_path = self.cached_path(path)
        if not cached_path.is_file() or cached_path.stat().st_size == 0:
            cached_path.parent.mkdir(parents=True, exist_ok=True)
            logger.debug("Copying file to cache: %s", path)
            super().get(str(path), str(cached_path))
        return cached_path

    def open(self, filename, mode="r", bufsize=-1):
        if hasattr(self, "__inside_open"):
            return super().open(filename=str(filename), mode=mode, bufsize=bufsize)
        setattr(self, "__inside_open", True)
        cached_path = self.ensure_cache(Path(filename))
        fr = open(cached_path, mode)
        setattr(fr, "prefetch", lambda _: None)
        delattr(self, "__inside_open")
        return fr


class SFTPClient(CachingSFTPClient):
    def copy_to_dir(self, src: Path, dst: Path):
        """Download to directory"""
        return self.get(str(src), str(dst / src.name))

    def decompress(self, fp: Path, attr, func: T.Callable):
        with open(fp, "rb") if self.cached_path(fp).is_file() else sftp.open(str(fp)) as fr:
            if isinstance(fr, paramiko.SFTPFile):
                fr.prefetch(attr.st_size)  # like in getfo() implementation
            return decompress(fr, fp, func)

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
            yield sftp_dir(asset_class, frequency, bucket, template=template, prefix=prefix)

    def ls(self, dir):
        return self.listdir_attr(str(dir))

    def matching(
        self, dir: Path, period: Period, copied_period: T.Optional[Period]
    ) -> T.Iterable[T.Tuple[paramiko.SFTPAttributes, Period]]:
        try:
            listing = self.ls(dir)
        except FileNotFoundError as e:
            logger.warning(f"{dir}: {e}")
            return
        logger.debug(f"Searching in {dir}")
        for attr in listing:
            file_period = parse_file_period(attr.filename)
            if overlaps(period, file_period) and not is_subperiod(file_period, copied_period):
                yield attr, file_period

    def download(
        self,
        asset_class: AssetClass,
        frequency: Frequency,
        start: datetime,
        end: datetime,
        output: T.Union[Output, str] = "pandas://",
        assets: T.Tuple[str, ...] = (),
        sources: T.Tuple[str, ...] = (),
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
        :param assets: Filter by asset (implemented only for DataFrameOutput)
        :param sources: Filter by dataType (implemented only for DataFrameOutput)
        :param buckets: Restrict search to given directories (daily / minutely / hourly).
        If empty (default), then search in all directories.
        :param prefix: Root folder on remote side
        :param trial: if true, append TRIAL to prefix
        :param template: Template for directory structure.
        If empty, template will be detected based on directory listing.
        If not empty, these variables will be substituted: prefix, asset_class, frequency, bucket.
        :returns: dataframe if output is DataFrameOutput
        """
        output = output if isinstance(output, Output) else Output.parse(output)
        if isinstance(output, DataFrameOutput):
            output = replace(output, assets=assets, start=start, end=end, sources=sources)
        copied_period = None
        n_files = 0
        result = None
        for dir in self.iter_dirs(
            asset_class=asset_class,
            frequency=frequency,
            buckets=buckets,
            template=template,
            prefix=prefix / ("TRIAL" if trial else ""),
        ):
            matching = tuple(self.matching(dir, (start, end), copied_period))
            attrs, periods = zip(*matching) if matching else ([], [])
            logger.info(f"Found {len(attrs)} files in {dir}")
            for attr in attrs:
                fp = dir / attr.filename  # type:ignore
                logger.info(f"Getting {fp}")
                result = output.copy_file(self, fp, attr, accum=result)
            copied_period = periods_union((copied_period, *periods))  # type:ignore
            n_files += len(attrs)
        if n_files == 0:
            logger.warning("No files found within time range")
        else:
            logger.debug(f"Processed {n_files} files within period: {copied_period}")
        return result

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
    user: T.Union[str, int],
    key: T.Union[None, Path, T.TextIO] = None,
    host: str = DEFAULT_HOST,
    cache: Path = DEFAULT_CACHE,
) -> SFTPClient:
    """
    Connect to host using credentials and return SFTPClient
    :param user: User ID as string
    :param key: Private key file object or filepath. If None, will use SSH_DIR/user
    :param host: SFTP server hostname
    """
    transport = paramiko.Transport(host)
    transport.connect(username=str(user), pkey=load_private_key(key or SSH_DIR / f"{user}.ppk"))
    client: T.Optional[SFTPClient] = SFTPClient.from_transport(transport)  # type:ignore
    if client is None:
        raise Exception("Couldn't connect to SFTP for some reason")
    client.cache = cache
    return client


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
    cli.add_argument("--assets", type=lambda s: s.split(","), help="Comma-separated list of assets")
    cli.add_argument(
        "--sources", type=lambda s: s.split(","), help="Comma-separated list of sources"
    )
    cli.enum_arg(Bucket, "--buckets", "-b", action="append", help="Restrict to these time buckets")
    cli.add_argument("--host", default=DEFAULT_HOST, help="Hostname of SFTP server")
    cli.add_argument("--cache", default=DEFAULT_CACHE, help="Cache dir, empty for no cache")
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
    with connect(user=args.user, key=args.key, host=args.host, cache=args.cache) as sftp:
        result = sftp.download(
            asset_class=args.asset_class,
            frequency=args.frequency,
            buckets=args.buckets,
            assets=args.assets,
            sources=args.sources,
            template=args.template,
            prefix=args.prefix,
            trial=args.trial,
            output=args.output,
            start=start,
            end=end,
        )
        if result is not None:
            if hasattr(result, "memory_usage"):
                logger.info("Memory usage: %dk", int(result.memory_usage(deep=True).sum() / 1024))
            print(result)

