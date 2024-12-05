%global __os_install_post %{nil}

Name:            redisclustertool
Version:         1.1
Release:         1%{?dist}
BuildArch:       x86_64
AutoReqProv:     no
Summary:         various redis cluster helpers
License:         MIT
URL:             https://github.com/rumanzo/redisclustertool
BuildRoot:       %{_tmppath}/%{name}-%{version}-buildroot
Requires   :     python3-redis
Source0:         https://github.com/rumanzo/%{name}/archive/refs/tags/%{version}.tar.gz


%description
Tool for monitoring and level out redis cluster

%prep
%setup -q

%install
%{__mkdir_p} %{buildroot}/etc/redisclustertool/
install -p -m 0640 -D config.cfg %{buildroot}%{_sysconfdir}/redisclustertool/
install -p -m 0755 -D redis-drain.sh %{buildroot}%{_bindir}/redis-drain.sh
install -p -m 0755 -D redisclustertool.py %{buildroot}%{_bindir}/redisclustertool.py

%clean
%{__rm} -rf %{buildroot}/*

%files
%{_bindir}/*
%config(noreplace) %{_sysconfdir}/redisclustertool/config.cfg
