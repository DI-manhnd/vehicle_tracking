import gi
gi.require_version('Gst','1.0')
gi.require_version('GstVideo','1.0')
gi.require_version('GstRtspServer','1.0')
from gi.repository import GObject, Gst, GstVideo, GstRtspServer

Gst.init(None)

mainloop = GObject.MainLoop()
server = GstRtspServer.RTSPServer()
mounts = server.get_mount_points()

factory = GstRtspServer.RTSPMediaFactory()
factory.set_launch('( v4l2src device=/dev/video0 ! video/x-raw,format=YUY2,width=640,height=480,framerate=30/1 ! nvvidconv ! nvv4l2h264enc insert-sps-pps=1 idrinterval=30 insert-vui=1 ! rtph264pay name=pay0 )')

mounts.add_factory("/test", factory)
server.attach(None)

print("stream ready at rtsp://127.0.0.1:8554/test")
mainloop.run()