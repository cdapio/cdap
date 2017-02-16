source 'https://supermarket.chef.io'

if Chef::VERSION.to_f < 12.0
  cookbook 'apt', '< 4.0'
  cookbook 'build-essential', '< 3.0'
  cookbook 'homebrew', '< 3.0'
  cookbook 'mingw', '< 1.0'
  cookbook 'ohai', '< 4.0'
  cookbook 'yum', '< 4.0'
  cookbook 'yum-epel', '< 2.0'
elsif Chef::VERSION.to_f < 12.5
  cookbook 'apt', '< 6.0'
  cookbook 'build-essential', '< 8.0'
  cookbook 'yum', '< 5.0'
elsif Chef::VERSION.to_f < 12.9
  cookbook 'apt', '< 6.0'
  cookbook 'yum', '< 5.0'
elsif Chef::VERSION.to_f < 12.14
  cookbook 'yum', '< 5.0'
end

metadata
