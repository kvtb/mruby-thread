MRuby::Gem::Specification.new('mruby-thread') do |spec|
  spec.license = 'MIT'
  spec.authors = 'mattn'

  spec.cxx.flags << '-fpermissive'

  if build.toolchains.include?('androideabi')
    spec.cc.defines << 'HAVE_PTHREADS'
  else
    spec.linker.flags << '-pthread'
  end
end
