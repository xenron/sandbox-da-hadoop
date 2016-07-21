#!/usr/bin/ruby

STDIN.read.split("\n").each do |line|
  line.split("\t").each do |field|
    field.split(" ").each do |word|
      puts "#{word}\t#{word.length}"
    end
  end
end
