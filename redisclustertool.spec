%global __os_install_post %{nil}
%define venv redisclusterhelpers_venv
%define conda_pack_ver 0.6.0
%define miniconda_ver Miniconda3-py311_23.9.0-0-Linux-x86_64.sh

Name:            redisclustertool
Version:         1.0
Release:         1%{?dist}
BuildArch:       x86_64
AutoReqProv:     no
Summary:         various redis cluster helpers
License:         MIT
URL:             https://github.com/rumanzo/redisclustertool
BuildRoot:       %{_tmppath}/%{name}-%{version}-buildroot
Source0:         https://github.com/rumanzo/%{name}/archive/refs/tags/%{version}.tar.gz
BuildRequires:   git curl
Source1:         https://repo.anaconda.com/miniconda/%{miniconda_ver}


%description
Tool for monitoring and level out redis cluster

%prep
bash %{SOURCE1} -b -p ./miniconda
tar xfv %{SOURCE0}
sed -i 's|#!/usr/bin/python3|#!/opt/%{venv}/bin/python|' %{name}-%{version}/*.py

%build
./miniconda/bin/conda install -y conda-pack=%{conda_pack_ver}
./miniconda/bin/conda env create -p %{venv} -f %{name}-%{version}/environment.yml
./miniconda/bin/conda-pack -p %{venv} -f --dest-prefix /opt/%{venv} --arcroot %{venv}

%install
%{__mkdir_p} %{buildroot}/opt
%{__mkdir_p} %{buildroot}/etc/redisclustertool/
tar xfv %{venv}.tar.gz -C %{buildroot}/opt/
install -p -m 0640 -D %{name}-%{version}/config.cfg %{buildroot}%{_sysconfdir}/redisclustertool/
install -p -m 0755 -D %{name}-%{version}/redis-drain.sh %{buildroot}%{_bindir}/redis-drain.sh
install -p -m 0755 -D %{name}-%{version}/redisclustertool.py %{buildroot}%{_bindir}/redisclustertool.py

%clean
./miniconda/bin/conda env remove -p %{venv}
%{__rm} %{venv}.tar.gz
%{__rm} -rf %{buildroot}/*

%files
%{_bindir}/*
/opt/%{venv}/*
%config(noreplace) %{_sysconfdir}/redisclustertool/config.cfg
