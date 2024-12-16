assert('Migrate') do

  class Class
   class AAA
    def to_x; "X" end
    class Simpl < AAA
   	 def to_i; 1234 end
     def to_s; "xx" end
    end
   end
  end

  $xxx = Class::AAA::Simpl.new

  def cmd_xxx
    "xxx"
  end

  $cmd_yyy = -> { "yyy" }

  $q = Queue.new
  $t = Thread.new($q) do |q|
    loop do
      op, *rest = q.shift
      p op
      case op
      when :runme
        assert_equal rest[0].call, "xxx"
        assert_true  $xxx.nil?       # complex type, not migrated
        $xxx = Class::AAA::Simpl.new # so we will make an own
        assert_equal $xxx.to_s, "xx"
        assert_equal $xxx.to_i, 1234
        assert_equal $xxx.to_x, "X"
        assert_equal $xxx.class.to_s,            "Class::AAA::Simpl"
        assert_equal $xxx.class.superclass.to_s, "Class::AAA"
        assert_equal $xxx.class,                 Class::AAA::Simpl
        assert_equal $xxx.class.superclass,      Class::AAA
      when :global
        assert_equal cmd_xxx,       "xxx"
        assert_equal $cmd_yyy.call, "yyy"
      end
    end
  end

  $q.push [:runme, lambda {cmd_xxx} ]
  $q.push [:global]

end
