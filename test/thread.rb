##
# Thread test

assert('Object.const_defined? :Thread') do
  assert_true Object.const_defined?(:Thread)
end

assert('Thread returns Fixnum') do
  a = Thread.new { 100 }
  assert_equal 100, a.join
end

assert('Thread returns true') do
  a = Thread.new { true }
  assert_true a.join
end

assert('Thread returns false') do
  a = Thread.new { false }
  assert_false a.join
end

assert('Thread returns Float') do
  a = Thread.new { 99.99 }
  assert_equal 99.99, a.join
end

assert('Thread returns String') do
  a = Thread.new { 'hello' }
  assert_equal 'hello', a.join
end

assert('Thread returns Symbol') do
  a = Thread.new { :context }
  assert_equal :context, a.join
end

assert('Thread returns Array') do
  a = Thread.new { [1, 2, 3] }
  assert_equal [1, 2, 3], a.join
end

assert('Thread returns Hash') do
  a = Thread.new { { 'abc_key' => 'abc_value', 'cba_key' => 'cba_value' } }
  assert_equal({ 'abc_key' => 'abc_value', 'cba_key' => 'cba_value' }, a.join)
end

assert('Thread migrates Fixnum') do
  a = Thread.new(100) { |v| v }
  assert_equal 100, a.join
end

assert('Thread migrates ture') do
  a = Thread.new(true) { |v| v }
  assert_true a.join
end

assert('Thread migrates false') do
  a = Thread.new(false) { |v| v }
  assert_false a.join
end

assert('Thread migrates Float') do
  a = Thread.new(99.99) { |v| v }
  assert_equal 99.99, a.join
end

assert('Thread migrates String') do
  a = Thread.new('hello') { |v| v }
  assert_equal 'hello', a.join
end

assert('Thread migrates Symbol') do
  a = Thread.new(:context) { |v| v }
  assert_equal :context, a.join
end

assert('Thread migrates Symbol in a complex context') do
  a = :cxt1
  t = Thread.new(:cxt2) { |b| [a, b, :ctx3] }
  assert_equal %i[cxt1 cxt2 ctx3], t.join
end

assert('Thread migrates Array') do
  skip 'skip because COPY_VALUES is disabled' unless Thread::COPY_VALUES
  a = Thread.new([1, 2, 3]) { |v| v }
  assert_equal [1, 2, 3], a.join
end

assert('Thread migrates Hash') do
  skip 'skip because COPY_VALUES is disabled' unless Thread::COPY_VALUES
  a = Thread.new({ 'abc_key' => 'abc_value', 'cba_key' => 'cba_value' }) { |v| v }
  assert_equal({ 'abc_key' => 'abc_value', 'cba_key' => 'cba_value' }, a.join)
end

assert('Thread migrates Proc') do
  pr = -> { 1 }
  a = Thread.new(pr) { |v| v.call }
  assert_equal 1, a.join
end

class DummyObj
  attr_accessor :foo, :bar, :buz
end

assert('Thread migrates Object') do
  t = Thread.new(DummyObj.new) do |v|
    v.foo = 'foo'
    v.bar = 123
    v.buz = :buz
    v
  end

  a = t.join

  assert_equal DummyObj, a.class
  assert_equal 'foo',    a.foo
  assert_equal 123,      a.bar
  assert_equal :buz,     a.buz
end


class DummyObj3
  def nnn; "NN" end
end

assert('Thread migrates methods') do
  t = Thread.new(DummyObj3.new) do |v| [v, v.nnn] end
  a, an = t.join
  assert_equal DummyObj3, a.class
  assert_equal 'NN',      a.nnn
  assert_equal 'NN',      an
end

assert('Thread accesses methods') do
  t = Thread.new do x = DummyObj3.new; [x, x.nnn] end
  a, an = t.join
  assert_equal DummyObj3, a.class
  assert_equal 'NN',      a.nnn
  assert_equal 'NN',      an
end


class DummyObj2
  def self.mmm; "MM" end
end

assert('Thread migrates self methods') do
  t = Thread.new(DummyObj2) do |v| [v, v.mmm] end
  a, am = t.join
  assert_equal DummyObj2, a
  assert_equal 'MM',      am
  assert_equal 'MM',      a.mmm
end

assert('Thread accesses self methods') do
  t = Thread.new do [DummyObj2, DummyObj2.mmm] end
  a, am = t.join
  assert_equal DummyObj2, a
  assert_equal 'MM',      am
  assert_equal 'MM',      a.mmm
end


assert('Thread proc captures a var') do
  x = 42
  a = Thread.new { x }
  assert_equal 42, a.join
end

assert('Thread proc does not capture a var') do
  x = 42
  a = Thread.new(capture: false) { x }
  assert_equal nil, a.join
end

assert('Fixed test of issue #36') do
  a = Thread.new { ''.is_a?(String) }
  assert_true a.join
end

assert('Thread GC') do
  t = Thread.new { GC.start; :end }
  assert_equal :end, t.join
end

assert('Thread sleep') do
  assert_nil Thread.sleep(1)
end

assert('Thread usleep') do
  assert_nil Thread.usleep(1)
rescue NotImplementedError => e
  skip e.message
end
