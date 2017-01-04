source 'https://supermarket.chef.io'

# Restrict version due to Chef 12 requirement
if RUBY_VERSION.to_f < 2.0
  cookbook 'apt', '< 4.0'
  cookbook 'build-essential', '< 3.0'
  cookbook 'homebrew', '< 3.0'
  cookbook 'mingw', '< 1.0'
  cookbook 'ohai', '< 4.0'
  cookbook 'yum-epel', '< 2.0'
end

metadata
