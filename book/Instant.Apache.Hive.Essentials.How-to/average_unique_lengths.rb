#!/usr/bin/ruby

last_key = nil
total = 0
count = 0

STDIN.read.split("\n").each do |line|
  fields = line.split("\t")
  key = fields[0]
  length = fields[1]
  if (key != last_key)
    last_key = key
    total += length.to_i
    count += 1
  end 
end

puts total.to_f / count
