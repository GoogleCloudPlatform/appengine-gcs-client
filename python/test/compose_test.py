""" cloudstorage compose tester"""
import webapp2
import logging
import re
import cloudstorage
from google.appengine.api import app_identity
from cloudstorage import errors

def alphanum_key(in_string):
  """Turn a string into a list of string and number chunks.
      "z23a" -> ["z", 23, "a"]
  """
  return [int(char) if char.isdigit() else char for char in re.split('([0-9]+)', in_string)]


# pylint: disable=too-few-public-methods
class MainPage(webapp2.RequestHandler):
  """Used to test the cloudstorage compose method."""

  # pylint: disable=too-many-locals, too-many-statements
  def get(self):
    """Main method to start the tests"""
    test_sizes_to_run = [1, 2, 3, 31, 32, 33]

    bucket_name = app_identity.get_default_gcs_bucket_name()
    the_list = []
    for item in cloudstorage.listbucket("/%s/input/file" % bucket_name):
      the_list.append(item.filename)

    the_list = sorted(the_list, key=alphanum_key)

    the_list = the_list[:max(test_sizes_to_run)]
    for i in range(len(the_list), max(test_sizes_to_run)):
      file_name = "/" + bucket_name + "/input/file%i.txt" % i
      logging.info("Creating File: %s ", file_name)

      with cloudstorage.open(file_name, "w") as gcs_file:
        gcs_file.write("%s\r\n" % str(i))
      the_list.append(file_name)
    list_of_files_string = []
    for item in the_list:
      file_name = item.replace("/" + bucket_name + "/", "")
      list_of_files_string.append(file_name)

    list_of_files_string.sort(key=alphanum_key)
    stats = ""
    output = ""
    stats, output = do_test(test_sizes_to_run, "/" + bucket_name
                            + "/results/compose_%i.txt",
                            list_of_files_string, cloudstorage.compose,
                            stats=stats, output=output)

    self.response.out.write("<HTML><BODY>")
    self.response.out.write(stats)
    self.response.out.write("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@</p>")
    self.response.out.write(output)
    self.response.out.write("</BODY></HTML>")


# pylint: disable=too-many-arguments
def do_test(test_sizes_to_run, template,
            list_of_files_string, func_to_call, stats='', output='', content_type=None):
  """Runs the test"""
  for test_size in test_sizes_to_run:
    final_file = template % test_size

    stats += "%s: " % final_file
    try:
      func_to_call(list_of_files_string[:test_size], final_file, content_type=content_type)
      try:
        output += final_file + "</p>"
        counter = 0
        with cloudstorage.open(final_file, "r") as gcs_source:
          line = gcs_source.readline()
          while line:
            output += line + "</p>"
            counter += 1
            line = gcs_source.readline()
        stats += str(counter) + "</p>"
      except errors.NotFoundError:
        stats += "Error opening file" + "</p>"
        output += "Error opening file"
    except (errors.NotFoundError, ValueError, TypeError) as the_error:
      stats += "Compose threw: %s" % str(the_error) + "</p>"
      output += "Compose threw: %s" % str(the_error) + "</p>"
    output += "********************************************************************</p>"

  return stats, output


# pylint: disable=invalid-name
app = webapp2.WSGIApplication([
    ('/', MainPage),
], debug=True)
