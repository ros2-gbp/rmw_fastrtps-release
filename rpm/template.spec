%bcond_without tests
%bcond_without weak_deps

%global __os_install_post %(echo '%{__os_install_post}' | sed -e 's!/usr/lib[^[:space:]]*/brp-python-bytecompile[[:space:]].*$!!g')
%global __provides_exclude_from ^/opt/ros/jazzy/.*$
%global __requires_exclude_from ^/opt/ros/jazzy/.*$

Name:           ros-jazzy-rmw-fastrtps-cpp
Version:        8.4.2
Release:        1%{?dist}%{?release_suffix}
Summary:        ROS rmw_fastrtps_cpp package

License:        Apache License 2.0
Source0:        %{name}-%{version}.tar.gz

Requires:       ros-jazzy-ament-cmake
Requires:       ros-jazzy-fastcdr
Requires:       ros-jazzy-fastrtps
Requires:       ros-jazzy-fastrtps-cmake-module
Requires:       ros-jazzy-rcpputils
Requires:       ros-jazzy-rcutils
Requires:       ros-jazzy-rmw
Requires:       ros-jazzy-rmw-dds-common
Requires:       ros-jazzy-rmw-fastrtps-shared-cpp
Requires:       ros-jazzy-rosidl-dynamic-typesupport
Requires:       ros-jazzy-rosidl-dynamic-typesupport-fastrtps
Requires:       ros-jazzy-rosidl-runtime-c
Requires:       ros-jazzy-rosidl-runtime-cpp
Requires:       ros-jazzy-rosidl-typesupport-fastrtps-c
Requires:       ros-jazzy-rosidl-typesupport-fastrtps-cpp
Requires:       ros-jazzy-tracetools
Requires:       ros-jazzy-ros-workspace
BuildRequires:  ros-jazzy-ament-cmake-ros
BuildRequires:  ros-jazzy-fastcdr
BuildRequires:  ros-jazzy-fastrtps
BuildRequires:  ros-jazzy-fastrtps-cmake-module
BuildRequires:  ros-jazzy-rcpputils
BuildRequires:  ros-jazzy-rcutils
BuildRequires:  ros-jazzy-rmw
BuildRequires:  ros-jazzy-rmw-dds-common
BuildRequires:  ros-jazzy-rmw-fastrtps-shared-cpp
BuildRequires:  ros-jazzy-rosidl-dynamic-typesupport
BuildRequires:  ros-jazzy-rosidl-dynamic-typesupport-fastrtps
BuildRequires:  ros-jazzy-rosidl-runtime-c
BuildRequires:  ros-jazzy-rosidl-runtime-cpp
BuildRequires:  ros-jazzy-rosidl-typesupport-fastrtps-c
BuildRequires:  ros-jazzy-rosidl-typesupport-fastrtps-cpp
BuildRequires:  ros-jazzy-tracetools
BuildRequires:  ros-jazzy-ros-workspace
Provides:       %{name}-devel = %{version}-%{release}
Provides:       %{name}-doc = %{version}-%{release}
Provides:       %{name}-runtime = %{version}-%{release}
Provides:       ros-jazzy-rmw-implementation-packages(member)

%if 0%{?with_tests}
BuildRequires:  ros-jazzy-ament-cmake-gtest
BuildRequires:  ros-jazzy-ament-lint-auto
BuildRequires:  ros-jazzy-ament-lint-common
BuildRequires:  ros-jazzy-osrf-testing-tools-cpp
BuildRequires:  ros-jazzy-test-msgs
%endif

%if 0%{?with_weak_deps}
Supplements:    ros-jazzy-rmw-implementation-packages(all)
%endif

%description
Implement the ROS middleware interface using eProsima FastRTPS static code
generation in C++.

%prep
%autosetup -p1

%build
# In case we're installing to a non-standard location, look for a setup.sh
# in the install tree and source it.  It will set things like
# CMAKE_PREFIX_PATH, PKG_CONFIG_PATH, and PYTHONPATH.
if [ -f "/opt/ros/jazzy/setup.sh" ]; then . "/opt/ros/jazzy/setup.sh"; fi
mkdir -p .obj-%{_target_platform} && cd .obj-%{_target_platform}
%cmake3 \
    -UINCLUDE_INSTALL_DIR \
    -ULIB_INSTALL_DIR \
    -USYSCONF_INSTALL_DIR \
    -USHARE_INSTALL_PREFIX \
    -ULIB_SUFFIX \
    -DCMAKE_INSTALL_PREFIX="/opt/ros/jazzy" \
    -DAMENT_PREFIX_PATH="/opt/ros/jazzy" \
    -DCMAKE_PREFIX_PATH="/opt/ros/jazzy" \
    -DSETUPTOOLS_DEB_LAYOUT=OFF \
%if !0%{?with_tests}
    -DBUILD_TESTING=OFF \
%endif
    ..

%make_build

%install
# In case we're installing to a non-standard location, look for a setup.sh
# in the install tree and source it.  It will set things like
# CMAKE_PREFIX_PATH, PKG_CONFIG_PATH, and PYTHONPATH.
if [ -f "/opt/ros/jazzy/setup.sh" ]; then . "/opt/ros/jazzy/setup.sh"; fi
%make_install -C .obj-%{_target_platform}

%if 0%{?with_tests}
%check
# Look for a Makefile target with a name indicating that it runs tests
TEST_TARGET=$(%__make -qp -C .obj-%{_target_platform} | sed "s/^\(test\|check\):.*/\\1/;t f;d;:f;q0")
if [ -n "$TEST_TARGET" ]; then
# In case we're installing to a non-standard location, look for a setup.sh
# in the install tree and source it.  It will set things like
# CMAKE_PREFIX_PATH, PKG_CONFIG_PATH, and PYTHONPATH.
if [ -f "/opt/ros/jazzy/setup.sh" ]; then . "/opt/ros/jazzy/setup.sh"; fi
CTEST_OUTPUT_ON_FAILURE=1 \
    %make_build -C .obj-%{_target_platform} $TEST_TARGET || echo "RPM TESTS FAILED"
else echo "RPM TESTS SKIPPED"; fi
%endif

%files
/opt/ros/jazzy

%changelog
* Wed Mar 12 2025 Geoffrey Biggs <geoff@openrobotics.org> - 8.4.2-1
- Autogenerated by Bloom

* Thu Jun 27 2024 Geoffrey Biggs <geoff@openrobotics.org> - 8.4.1-1
- Autogenerated by Bloom

* Fri Apr 19 2024 Geoffrey Biggs <geoff@openrobotics.org> - 8.4.0-2
- Autogenerated by Bloom

* Tue Apr 09 2024 Geoffrey Biggs <geoff@openrobotics.org> - 8.4.0-1
- Autogenerated by Bloom

* Thu Mar 28 2024 Geoffrey Biggs <geoff@openrobotics.org> - 8.3.0-1
- Autogenerated by Bloom

* Wed Mar 06 2024 Geoffrey Biggs <geoff@openrobotics.org> - 8.2.0-2
- Autogenerated by Bloom

