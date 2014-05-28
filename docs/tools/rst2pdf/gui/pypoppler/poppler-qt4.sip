%Module QtPoppler 0

%Import QtCore/QtCoremod.sip
%Import QtGui/QtGuimod.sip
%Import QtXml/QtXmlmod.sip


// %Include poppler-link.sip
// %Include poppler-annotation.sip

%ModuleHeaderCode
#define UNSTABLE_POPPLER_QT4 1
#include <qt4/poppler-qt4.h>
#include <qt4/poppler-link.h>
%End

/**
   The Poppler Qt bindings
*/
namespace Poppler {

class LinkDestination {
%TypeHeaderCode
#define UNSTABLE_POPPLER_QT4 1
#include <qt4/poppler-qt4.h>
#include <qt4/poppler-link.h>
%End

public:
        LinkDestination(const QString &description);
        enum Kind
        {
            /**
             * The new viewport is specified in terms of:
             * - possibile new left coordinate (see isChangeLeft() )
             * - possibile new top coordinate (see isChangeTop() )
             * - possibile new zoom level (see isChangeZoom() )
             */
            destXYZ = 1,
            destFit = 2,
            destFitH = 3,
            destFitV = 4,
            destFitR = 5,
            destFitB = 6,
            destFitBH = 7,
            destFitBV = 8
        };
        Kind kind() const;
        int pageNumber() const;
        double left() const;
        double bottom() const;
        double right() const;
        double top() const;

};


class TextBox {

%TypeHeaderCode
#define UNSTABLE_POPPLER_QT4 1
#include <qt4/poppler-qt4.h>
%End

public:
    TextBox(const QString& text, const QRectF &bBox);
    const QString &text() const;
    const QRectF &boundingBox() const;
    Poppler::TextBox *nextWord() const;
    bool hasSpaceAfter() const;

private:
    TextBox( const Poppler::TextBox & );
};


class FontInfo {
%TypeHeaderCode
#define UNSTABLE_POPPLER_QT4 1
#include <qt4/poppler-qt4.h>
%End


public:
    enum Type {
        unknown = 0,
        Type1 = 1,
        Type1C = 2,
        Type3 = 3,
        TrueType = 4,
        CIDType0 = 5,
        CIDType0C = 6,
        CIDTrueType = 7
    };

//     FontInfo( const FontInfoData &fid );

    ~FontInfo();
    const QString &name() const;
    const QString &file() const;
    bool isEmbedded() const;
    bool isSubset() const;
    Type type() const;
    const QString typeName() const;
private:
    FontInfo();
//     FontInfo(const FontInfo &);
};

class EmbeddedFile {
%TypeHeaderCode
#define UNSTABLE_POPPLER_QT4 1
#include <qt4/poppler-qt4.h>
%End

public:
//     EmbeddedFile(EmbFile *embfile);

    ~EmbeddedFile();
    QString name() const;
    QString description() const;
    QDateTime modDate() const;
    QDateTime createDate() const;

    QByteArray data();
private:
    EmbeddedFile();
    EmbeddedFile( const Poppler::EmbeddedFile & );
};


class Page {
%TypeHeaderCode
#define UNSTABLE_POPPLER_QT4 1
#include <qt4/poppler-qt4.h>
%End

public:
    ~Page();

    QImage renderToImage(double xres=72.0, double yres=72.0, int x=-1, int y=-1, int w=-1, int h=-1) const;

    QString text(const QRectF &rect) const;
    
    
    enum SearchDirection { FromTop, NextResult, PreviousResult };
    enum SearchMode { CaseSensitive, CaseInsensitive };
    bool search(const QString &text, QRectF &rect, SearchDirection direction, SearchMode caseSensitive) const;

    QList<Poppler::TextBox*> textList() const;

    QSizeF pageSizeF() const;

    QSize pageSize() const;


//     PageTransition *transition() const;
    

    enum Orientation {
        Landscape, ///< Landscape orientation (portrait, with 90 degrees clockwise rotation )
        Portrait, ///< Normal portrait orientation
        Seascape, ///< Seascape orientation (portrait, with 270 degrees clockwise rotation)
        UpsideDown ///< Upside down orientation (portrait, with 180 degrees rotation)
    };


    Orientation orientation() const;
    

    void defaultCTM(double *CTM, double dpiX, double dpiY, int rotate, bool upsideDown);
    

//     QList<Link*> links() const;
//     QList<Annotation*> annotations () const;
    
private:
    Page();
    Page( const Poppler::Page & );
};

class Document {
%TypeHeaderCode
#define UNSTABLE_POPPLER_QT4 1
#include <qt4/poppler-qt4.h>
%End

    public:

    enum PageMode {
        UseNone,     ///< No mode - neither document outline nor thumbnail images are visible
        UseOutlines, ///< Document outline visible
        UseThumbs,   ///< Thumbnail images visible
        FullScreen,  ///< Fullscreen mode (no menubar, windows controls etc)
        UseOC,       ///< Optional content group panel visible
        UseAttach    ///< Attachments panel visible
    };
  
    enum PageLayout {
        NoLayout,   ///< Layout not specified
        SinglePage, ///< Display a single page
        OneColumn,  ///< Display a single column of pages
        TwoColumnLeft, ///< Display the pages in two columns, with odd-numbered pages on the left
        TwoColumnRight, ///< Display the pages in two columns, with odd-numbered pages on the right
        TwoPageLeft, ///< Display the pages two at a time, with odd-numbered pages on the left
        TwoPageRight, ///< Display the pages two at a time, with odd-numbered pages on the right
    };

    static Poppler::Document *load(const QString & filePath,
                  const QByteArray &ownerPassword=QByteArray(),
                  const QByteArray &userPassword=QByteArray());

    static Poppler::Document *loadFromData(const QByteArray & fileContents,
                    const QByteArray &ownerPassword=QByteArray(),
                    const QByteArray &userPassword=QByteArray());


    Poppler::Page *page(int index) const;

    Poppler::Page *page(QString label) const;


    int numPages() const;
  

    PageMode pageMode() const;


    PageLayout pageLayout() const;


    bool unlock(const QByteArray &ownerPassword, const QByteArray &userPassword);


    bool isLocked() const;


    QDateTime date( const QString & data ) const;


    QString info( const QString & data ) const;


    QStringList infoKeys() const;

    bool isEncrypted() const;

    bool isLinearized() const;

    bool okToPrint() const;

    bool okToPrintHighRes() const;

    bool okToChange() const;

    bool okToCopy() const;

    bool okToAddNotes() const;

    bool okToFillForm() const;

    bool okToCreateFormFields() const;

    bool okToExtractForAccessibility() const;

    bool okToAssemble() const;

    double pdfVersion() const;

    QList<Poppler::FontInfo> fonts() const;

    bool scanForFonts( int numPages, QList<Poppler::FontInfo> *fontList ) const;

    const QList<Poppler::EmbeddedFile*> &embeddedFiles() const;

    bool hasEmbeddedFiles() const;

    QDomDocument *toc() const;
    
    Poppler::LinkDestination *linkDestination( const QString &name );
    
//     bool print(const QString &fileName, const QList<int> pageList, double hDPI, double vDPI, int rotate);
    
    void setPaperColor(const QColor &color);
    QColor paperColor() const;
    enum RenderBackend {
        SplashBackend,   ///< Splash backend
        ArthurBackend   ///< Arthur (Qt4) backend
    };

    enum RenderHint {
        Antialiasing = 0x00000001,      ///< Antialiasing for graphics
        TextAntialiasing = 0x00000002   ///< Antialiasing for text
    };

//    void setRenderBackend( RenderBackend backend );

//    RenderBackend renderBackend() const;

//    static QSet<RenderBackend> availableRenderBackends();

    void setRenderHint( RenderHint hint, bool on = true );

   // RenderHints renderHints() const;
    ~Document();
  
private:
    Document();
    Document(const Poppler::Document &);
};

    QDateTime convertDate( char *dateString );

};

